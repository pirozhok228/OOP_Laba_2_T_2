#include <coroutine>
#include <boost/asio.hpp>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <exception>
#include <thread>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <vector>

namespace asio = boost::asio;
std::mutex logMutex;
using asio::awaitable;
using asio::use_awaitable;

struct Record {
    std::int64_t timestamp = 0;
    std::string message;
    std::size_t fileIndex = 0;
};

bool parseRecord(const std::string& line, std::size_t fileIndex, Record& record) {
    const auto separator = line.find_first_of(" \t");
    if (separator == std::string::npos) return false;

    try {
        std::size_t used = 0;
        const std::string timeText = line.substr(0, separator);
        record.timestamp = std::stoll(timeText, &used);
        if (used != timeText.size()) return false;
    } catch (...) {
        return false;
    }

    std::size_t messageStart = line.find_first_not_of(" \t", separator);
    record.message = messageStart == std::string::npos ? "" : line.substr(messageStart);
    record.fileIndex = fileIndex;
    return true;
}

class SyncLineReader {
public:
    explicit SyncLineReader(const std::string& path) : input(path) {
        if (!input) throw std::runtime_error("Не удалось открыть файл: " + path);
    }

    bool next(Record& record, std::size_t fileIndex) {
        std::string line;
        if (!std::getline(input, line)) return false;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!parseRecord(line, fileIndex, record))
            throw std::runtime_error("Некорректная запись в файле с индексом " + std::to_string(fileIndex));
        return true;
    }
private:
    std::ifstream input;
};

class AsyncLineReader {
public:
    AsyncLineReader(const std::string& path, std::size_t blockSize, asio::thread_pool& pool)
        : input(path, std::ios::binary), blockSize(blockSize), pool(pool) {
        if (!input) throw std::runtime_error("Не удалось открыть файл: " + path);
        if (blockSize == 0) throw std::runtime_error("Размер блока должен быть положительным");
    }

    awaitable<bool> next(Record& record, std::size_t fileIndex) {
        for (;;) {
            const auto newline = pending.find('\n');
            if (newline != std::string::npos) {
                std::string line = pending.substr(0, newline);
                pending.erase(0, newline + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                if (!parseRecord(line, fileIndex, record))
                    throw std::runtime_error("Некорректная запись в файле с индексом " + std::to_string(fileIndex));
                co_return true;
            }

            if (eof) {
                if (pending.empty()) co_return false;
                std::string line = std::move(pending);
                pending.clear();
                if (!line.empty() && line.back() == '\r') line.pop_back();
                if (!parseRecord(line, fileIndex, record))
                    throw std::runtime_error("Некорректная запись в файле с индексом " + std::to_string(fileIndex));
                co_return true;
            }

            co_await asio::post(pool, use_awaitable);
            std::string block(blockSize, '\0');
            input.read(block.data(), static_cast<std::streamsize>(block.size()));
            const auto count = input.gcount();
            if (count > 0) {
                pending.append(block.data(), static_cast<std::size_t>(count));
                std::lock_guard<std::mutex> lock(logMutex);
                std::cout << "Завершено чтение блока файла " << fileIndex
                          << ", байт: " << count << "\n";
            }
            if (input.eof()) eof = true;
            else if (!input && count == 0) throw std::runtime_error("Ошибка чтения входного файла");
        }
    }

private:
    std::ifstream input;
    std::size_t blockSize;
    asio::thread_pool& pool;
    std::string pending;
    bool eof = false;
};

struct LaterRecord {
    bool operator()(const Record& a, const Record& b) const {
        if (a.timestamp != b.timestamp) return a.timestamp > b.timestamp;
        return a.fileIndex > b.fileIndex;
    }
};

void mergeSync(const std::vector<std::string>& paths, const std::string& outputPath) {
    std::vector<std::unique_ptr<SyncLineReader>> readers;
    std::priority_queue<Record, std::vector<Record>, LaterRecord> queue;

    for (std::size_t i = 0; i < paths.size(); ++i) {
        readers.push_back(std::make_unique<SyncLineReader>(paths[i]));
        Record record;
        if (readers.back()->next(record, i)) queue.push(std::move(record));
    }

    std::ofstream output(outputPath);
    if (!output) throw std::runtime_error("Не удалось создать выходной файл");
    while (!queue.empty()) {
        Record current = queue.top();
        queue.pop();
        output << current.timestamp << '\t' << current.message << '\n';

        Record next;
        if (readers[current.fileIndex]->next(next, current.fileIndex))
            queue.push(std::move(next));
    }
    if (!output) throw std::runtime_error("Ошибка записи итогового журнала");
}

awaitable<void> mergeAsync(asio::io_context& io, asio::thread_pool& pool,
                           const std::vector<std::string>& paths,
                           const std::string& outputPath, std::size_t blockSize) {
    std::vector<std::unique_ptr<AsyncLineReader>> readers;
    for (const auto& path : paths)
        readers.push_back(std::make_unique<AsyncLineReader>(path, blockSize, pool));

    std::ofstream output(outputPath);
    if (!output) throw std::runtime_error("Не удалось создать выходной файл");

    std::priority_queue<Record, std::vector<Record>, LaterRecord> queue;

    struct InitialState {
        explicit InitialState(asio::io_context& context, std::size_t count)
            : timer(context), remaining(count), records(count), errors(count) {}
        asio::steady_timer timer;
        std::size_t remaining;
        std::vector<Record> records;
        std::vector<std::exception_ptr> errors;
    };

    auto state = std::make_shared<InitialState>(io, readers.size());
    if (!readers.empty()) {
        state->timer.expires_at(std::chrono::steady_clock::time_point::max());
        for (std::size_t i = 0; i < readers.size(); ++i) {
            asio::co_spawn(io, readers[i]->next(state->records[i], i),
                [state, i](std::exception_ptr error, bool hasRecord) {
                    state->errors[i] = error;
                    if (!error && !hasRecord) state->records[i].fileIndex = static_cast<std::size_t>(-1);
                    if (--state->remaining == 0) state->timer.cancel();
                });
        }
        boost::system::error_code ignored;
        co_await state->timer.async_wait(asio::redirect_error(use_awaitable, ignored));
        for (std::size_t i = 0; i < readers.size(); ++i) {
            if (state->errors[i]) std::rethrow_exception(state->errors[i]);
            if (state->records[i].fileIndex != static_cast<std::size_t>(-1))
                queue.push(std::move(state->records[i]));
        }
    }

    while (!queue.empty()) {
        Record current = queue.top();
        queue.pop();
        output << current.timestamp << '\t' << current.message << '\n';
        if (!output) throw std::runtime_error("Ошибка записи итогового журнала");

        Record next;
        const bool hasNext = co_await readers[current.fileIndex]->next(next, current.fileIndex);
        if (hasNext) queue.push(std::move(next));
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc < 5) {
            std::cout << "Использование:\n"
                      << "  log_merger sync <output.txt> <block_size> <input1> [input2 ...]\n"
                      << "  log_merger async <output.txt> <block_size> <input1> [input2 ...]\n";
            return 1;
        }

        const std::string mode = argv[1];
        const std::string outputPath = argv[2];
        const std::size_t blockSize = static_cast<std::size_t>(std::stoull(argv[3]));
        if (blockSize == 0) throw std::runtime_error("Размер блока должен быть положительным");

        std::vector<std::string> paths;
        for (int i = 4; i < argc; ++i) paths.emplace_back(argv[i]);
        if (paths.empty()) throw std::runtime_error("Не указаны входные файлы");

        const auto wallStart = std::chrono::system_clock::now();
        const auto start = std::chrono::steady_clock::now();
        if (mode == "sync") {
            mergeSync(paths, outputPath);
        } else if (mode == "async") {
            asio::io_context io;
            asio::thread_pool pool(std::max(2u, std::thread::hardware_concurrency()));
            std::exception_ptr asyncError;
            asio::co_spawn(io, mergeAsync(io, pool, paths, outputPath, blockSize),
                [&asyncError](std::exception_ptr error) { asyncError = error; });
            io.run();
            pool.join();
            if (asyncError) std::rethrow_exception(asyncError);
        } else {
            throw std::runtime_error("Режим должен быть sync или async");
        }

        const auto finish = std::chrono::steady_clock::now();
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(finish - start).count();
        std::cout << "Start: " << std::chrono::system_clock::to_time_t(wallStart) << "\n";
        std::cout << "End: " << std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) << "\n";
        std::cout << "Mode: " << mode << "\n";
        std::cout << "Count input files: " << paths.size() << "\n";
        std::cout << "Size of block: " << blockSize << " bytes\n";
        std::cout << "Result file: " << outputPath << "\n";
        std::cout << "Time: " << elapsed << " мс\n";
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}
