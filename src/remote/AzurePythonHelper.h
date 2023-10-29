//
// Created by Zhihan Guo on 10/12/23.
//
#ifndef README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_
#define README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_



#include "common/Common.h"

namespace arboretum {

static volatile int gil_init;

class AzurePythonHelper {

 public:
  explicit AzurePythonHelper(const std::string &conn_str) {
    auto cwd = std::filesystem::current_path().generic_string();
    std::ifstream pyin("configs/ifconfig_python.txt");
    std::stringstream ss;
    ss << cwd << "/python_deps";
    std::string line;
    while (getline(pyin, line)) {
      ss << ":" << line;
    }
    Py_SetProgramName(Py_DecodeLocale("python3", nullptr));
    Py_SetPath(Py_DecodeLocale(ss.str().c_str(), nullptr));
    Py_Initialize();
    gil_init = 0;
    auto module = PyImport_Import(PyUnicode_DecodeFSDefault("AzureClientPython"));
    auto dict = PyModule_GetDict(module);
    Py_DECREF(module);
    auto python_class = PyDict_GetItemString(dict, "PythonAzureClient");
    Py_DECREF(dict);
    object_ = PyObject_CallObject(python_class, nullptr);
    PyObject_CallMethodObjArgs(object_,
                               PyUnicode_FromString("create_conn"),
                               PyUnicode_FromString(conn_str.c_str()), NULL);
  }

  ~AzurePythonHelper() {
    Py_Finalize();
  }

  bool ExecPyRangeSearch(std::string &tbl_name, std::string &part,
                         std::string &low, std::string &high,
                         char **data_ptrs) {
    PyObject *pValue;
    if (!gil_init) {
      pyContextLock.lock();
      if (!gil_init) {
        gil_init = 1;
        PyEval_InitThreads();
        PyEval_SaveThread();
      }
      pyContextLock.unlock();
    }
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();
    // LOG_DEBUG("request for %s - %s received", low.c_str(), high.c_str());
    pValue = PyObject_CallMethodObjArgs(object_,
                                        PyUnicode_FromString("range_search"),
                                        PyUnicode_FromString(tbl_name.c_str()),
                                        PyUnicode_FromString(part.c_str()),
                                        PyUnicode_FromString(low.c_str()),
                                        PyUnicode_FromString(high.c_str()),
                                        NULL);
    if (pValue) {
      // PyList_Size
      auto return_sz = PyList_Size(pValue);
      // LOG_INFO("Returned %ld results\n", return_sz);
      for (Py_ssize_t i = 0; i < return_sz; i++) {
        auto obj = PyList_GetItem(pValue, i);
        // auto key_obj = PyDict_GetItem(obj, PyUnicode_FromString("RowKey"));
        // auto key = PyUnicode_AsUTF8(key_obj);
        // LOG_INFO("Row key of %zu-th item: %s\n", i, key);
        auto data_obj = PyDict_GetItem(obj, PyUnicode_FromString("Data"));
        memcpy(data_ptrs[i], PyBytes_AsString(data_obj), PyBytes_Size(data_obj));
        // LOG_INFO("key = %s, copy data to %p with size %zu\n",
        //         key, data_ptrs[i], PyBytes_Size(data_obj));
      }
      Py_DECREF(pValue);
    } else {
      PyErr_Print();
      LOG_ERROR("no pValue");
    }
    // LOG_DEBUG("request for %s - %s done", low.c_str(), high.c_str());
    PyGILState_Release(gstate);
    return true;
  }

  std::mutex pyContextLock;
  PyObject *object_;
};
}

#endif //README_MD_SRC_REMOTE_AZUREPYTHONHELPER_H_
