You can modify [config.mk](config.mk) to customize the building. You can copy
this file to the upper directory so that the changes will be ignored by git.

## FAQ

<!-- - `/usr/bin/ld: cannot find -lgssapi_krb5` -->
<!-- remove `-lgssapi_krb5` in [ps.mk](https://github.com/dmlc/ps-lite/blob/master/make/ps.mk) -->

- `undefined reference to `_Ux86_64_getcontext'`

add `-lunwind` in makefile (e.g `LDFLAGS += -lunwind`)
