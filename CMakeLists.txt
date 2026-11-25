
cmake_minimum_required(VERSION 3.20)

project(log_merger LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost CONFIG REQUIRED)

find_package(Threads REQUIRED)

add_executable(log_merger main.cpp)

target_link_libraries(log_merger PRIVATE
    Boost::headers
    Threads::Threads
)

target_compile_definitions(log_merger PRIVATE
    _WIN32_WINNT=0x0A00
    BOOST_ASIO_HAS_CO_AWAIT
)
