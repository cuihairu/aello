#pragma once

// 跨平台共享库导出/导入宏
// 用于在 Windows 上正确导出符号，在其他平台上无害

// 默认情况下，假设我们正在构建库
#if defined(_WIN32) || defined(__CYGWIN__)
  #ifdef DUCT_STATIC
    #define DUCT_API
    #define DUCT_PRIVATE
  #else
    #ifdef duct_EXPORTS
      #define DUCT_API __declspec(dllexport)
    #else
      #define DUCT_API __declspec(dllimport)
    #endif
    #define DUCT_PRIVATE
  #endif
#else
  #if __GNUC__ >= 4
    #define DUCT_API __attribute__((visibility("default")))
    #define DUCT_PRIVATE __attribute__((visibility("hidden")))
  #else
    #define DUCT_API
    #define DUCT_PRIVATE
  #endif
#endif

// 标记为实验性 API
#define DUCT_EXPERIMENTAL [[deprecated("This is an experimental API and may change in future releases.")]]

// 标记为内部实现细节
#define DUCT_INTERNAL DUCT_PRIVATE

// C++ 语言链接
#ifdef __cplusplus
#define DUCT_EXTERN_C extern "C"
#else
#define DUCT_EXTERN_C
#endif
