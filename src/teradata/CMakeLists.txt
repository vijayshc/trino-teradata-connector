cmake_minimum_required(VERSION 3.10)
project(TeradataExportVantage)

set(CMAKE_CXX_STANDARD 17)

# Include Teradata mock headers
include_directories(${CMAKE_CURRENT_SOURCE_VALUE}/include)

# Find Apache Arrow and Flight
# In a real environment, you would use find_package(Arrow)
# For now, we'll setup include paths manually if not found.
find_package(Arrow CONFIG)
find_package(ArrowFlight CONFIG)

if(NOT Arrow_FOUND)
    message(WARNING "Arrow not found. Compilation will fail unless include paths are provided via -DARROW_INCLUDE_DIR")
endif()

add_library(teradata_export SHARED teradata_export.cpp)

target_include_directories(teradata_export PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${ARROW_INCLUDE_DIR}
)

if(Arrow_FOUND)
    target_link_libraries(teradata_export PRIVATE arrow_shared arrow_flight_shared)
endif()

# UDFs should be compiled with position-independent code
set_target_properties(teradata_export PROPERTIES POSITION_INDEPENDENT_CODE ON)
