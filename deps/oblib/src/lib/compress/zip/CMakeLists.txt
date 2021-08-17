# Define object library zip
add_library(zip_objs OBJECT
  zip.c)

target_compile_options(zip_objs PRIVATE -fvisibility=hidden)
target_link_libraries(zip_objs PUBLIC oblib_base_base)
disable_pch(zip_objs)

add_custom_command(
  OUTPUT zip_objs.o
  COMMAND ${LD_BIN} -r $<TARGET_OBJECTS:zip_objs> -o zip_objs.tmp.o
  COMMAND ${OBJCOPY_BIN} --localize-hidden zip_objs.tmp.o zip_objs.o
  DEPENDS zip_objs
  BYPRODUCTS zip_objs.o
  COMMAND_EXPAND_LISTS
  )

oblib_add_extra_objects(${CMAKE_CURRENT_BINARY_DIR}/zip_objs.o)
ob_lib_add_target(oblib_compress_zip zip_objs.o)
