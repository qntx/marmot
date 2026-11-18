project(MarmotNative)
cmake_minimum_required(VERSION 3.9.0)

set(PACKAGE_NAME MarmotNative)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 20)

# The C++ hybrids are the only thing we compile here: libmarmot_ffi.so is a
# prebuilt mk-ffi cdylib per ABI under src/main/jniLibs (built by
# `bun run build:native`), packaged into the AAR and linked for its mk_* symbols.
file(GLOB shared_files RELATIVE ${CMAKE_SOURCE_DIR}
     "../cpp/**.cpp" "src/main/cpp/**.cpp"
)

add_library(${PACKAGE_NAME} SHARED ${shared_files})

# Add Nitrogen specs :)
include(${CMAKE_SOURCE_DIR}/../nitrogen/generated/android/MarmotNative+autolinking.cmake)

add_library(marmot_ffi SHARED IMPORTED)
set_target_properties(
        marmot_ffi
        PROPERTIES IMPORTED_LOCATION
        ${CMAKE_SOURCE_DIR}/src/main/jniLibs/${ANDROID_ABI}/libmarmot_ffi.so
)

include_directories(
        "../cpp"
        "../include"
)

find_library(LOG_LIB log)

target_link_libraries(
        ${PACKAGE_NAME}
        marmot_ffi
        ${LOG_LIB}
        android
)
