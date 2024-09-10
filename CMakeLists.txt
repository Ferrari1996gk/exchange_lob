cmake_minimum_required(VERSION 3.15)
project(SimulatedMarket)

#set(CMAKE_CXX_COMPILER /usr/local/bin/g++-8)
#set(CMAKE_C_COMPILER "/usr/bin/gcc-10" CACHE STRING "C++ compiler" FORCE)
set(CMAKE_CXX_STANDARD 17)

add_executable(SimulatedMarket main.cpp cda_engine.h cda_engine.cpp agentBase.cpp agentBase.h simulator.cpp simulator.h params.h fundamentalTrader.h fundamentalTrader.cpp fundamentalValue.h momentumTrader.h momentumTrader.cpp ziTrader.h ziTrader.cpp momentumValue.h hmomentumTrader.h hmomentumTrader.cpp marketMaker.h marketMaker.cpp insTrader.h insTrader.cpp spikeTrader.h spikeTrader.cpp)

#set(BOOST_ROOT "../../path_to_boost_1_70_0")
#set(BOOST_ROOT "../boost_1_75_0")

set(Boost_ARCHITECTURE -x64)
set(BOOST_ROOT D:/install_package/boost)
set(Boost_INCLUDE_DIR D:/install_package/boost/include/boost-1_75)

message(STATUS "start running cmake...")

find_package(Boost 1.75.0 COMPONENTS system filesystem REQUIRED)

if(NOT Boost_FOUND)
    message(FATAL_ERROR "Could not find boost!")
endif()

if(Boost_FOUND)

    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
    message(STATUS "Boost_VERSION: ${Boost_VERSION}")

    include_directories(${Boost_INCLUDE_DIRS})

    target_link_libraries(SimulatedMarket ${Boost_LIBRARIES})

endif()

find_package(OpenMP)

#if(OpenMP_CXX_FOUND)
#    target_link_libraries(MyTarget PUBLIC OpenMP::OpenMP_CXX)
#endif()