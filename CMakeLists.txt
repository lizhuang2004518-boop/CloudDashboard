# ============================================================================
#  云端考勤管理与数据大屏 —— CMake 构建脚本
#
#  依赖：
#    - Qt 5.14.2（Widgets / Network / Charts / WebSockets）
#      Ubuntu 安装：sudo apt install qtbase5-dev libqt5charts5-dev
#      （Charts 是独立模块，必须装 libqt5charts5-dev，否则 find_package 会失败）
#    - OpenCV 4.5.4（core / imgproc / imgcodecs / dnn / objdetect）
#      源码安装在 /usr/local
#
#  构建：
#    mkdir -p build && cd build
#    cmake .. && make -j$(nproc)
#    ./CloudDashboard
# ============================================================================

cmake_minimum_required(VERSION 3.10)
project(CloudDashboard VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Qt 的元对象编译器 / 资源编译（MainWindow 里有 Q_OBJECT，必须开 AUTOMOC）
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)
# 界面是纯代码写的，不依赖 .ui 文件；若以后改用 Qt Designer 再打开
set(CMAKE_AUTOUIC OFF)
set(CMAKE_INCLUDE_CURRENT_DIR ON)

# ---------------------------------------------------------------------------
# 1) Qt5：Charts 和 WebSockets 都是独立模块，缺一个都会在 find_package 处报错
# ---------------------------------------------------------------------------
# 只要求 Qt5 + 四个模块，不写死小版本：
# 代码用到的接口在 Qt 5.12 及以后都有（setLabelsColor 是 5.12 加入），
# 这样 Ubuntu 20.04 自带的 5.12.8 与 Qt 在线安装器的 5.14.2 都能直接编。
find_package(Qt5 REQUIRED COMPONENTS Widgets Network Charts WebSockets)
message(STATUS "Qt5 版本 : ${Qt5Widgets_VERSION}")
message(STATUS "Qt5 路径 : ${Qt5Widgets_DIR}")

# ---------------------------------------------------------------------------
# 2) OpenCV：YuNet 检测与 SFace 识别都在 objdetect 模块里（4.5.4 起）
# ---------------------------------------------------------------------------
find_package(OpenCV 4 REQUIRED COMPONENTS core imgproc imgcodecs dnn objdetect)
message(STATUS "OpenCV   : ${OpenCV_VERSION}  (${OpenCV_DIR})")

# ---------------------------------------------------------------------------
# 3) 可执行文件
# ---------------------------------------------------------------------------
add_executable(CloudDashboard
    main.cpp
    mainwindow.cpp
    mainwindow.h
)

target_include_directories(CloudDashboard PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${OpenCV_INCLUDE_DIRS}
)

target_link_libraries(CloudDashboard PRIVATE
    Qt5::Widgets
    Qt5::Network
    Qt5::Charts
    Qt5::WebSockets
    ${OpenCV_LIBS}
)

# ---------------------------------------------------------------------------
# 4) 运行提示：模型目录既支持 models/ 也兼容仓库里现有的 modles/（拼写不影响加载）
# ---------------------------------------------------------------------------
message(STATUS "运行前请确认模型目录：<程序目录>/models 或 modles 下存在")
message(STATUS "    yunet.onnx  /  face_recognizer_fast.onnx")
