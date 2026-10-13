if(NOT TARGET SelfIPCs::self_ipcs)
  include("${CMAKE_CURRENT_LIST_DIR}/../SelfIPCs/SelfIPCsConfig.cmake")
endif()
if(NOT TARGET EdgeRuntime::edge_runtime)
  add_library(EdgeRuntime::edge_runtime INTERFACE IMPORTED)
  set_target_properties(EdgeRuntime::edge_runtime PROPERTIES
    INTERFACE_LINK_LIBRARIES SelfIPCs::self_ipcs)
endif()
