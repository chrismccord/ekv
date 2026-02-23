PRIV_DIR = $(MIX_APP_PATH)/priv
OUTPUT = $(PRIV_DIR)/ekv_sqlite3_nif.so

ERL_INCLUDE = $(ERTS_INCLUDE_DIR)

SRC = c_src/ekv_sqlite3_nif.c c_src/sqlite3.c

CFLAGS += -O2 -fPIC -I$(ERL_INCLUDE) -Ic_src \
  -DSQLITE_THREADSAFE=1 \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_DQS=0 \
  -DHAVE_USLEEP=1 \
  -DSQLITE_ENABLE_STAT4=1 \
  -DSQLITE_LIKE_DOESNT_MATCH_BLOBS=1 \
  -DNDEBUG=1

# Cross-compilation: cc_precompiler sets CROSSCOMPILE prefix
ifdef CROSSCOMPILE
  CC = $(CROSSCOMPILE)gcc
endif

# Platform-specific linker flags
# cc_precompiler sets TARGET_ABI; fall back to uname for native builds
ifndef TARGET_ABI
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    TARGET_ABI = darwin
  endif
endif

ifeq ($(TARGET_ABI),darwin)
  LDFLAGS += -dynamiclib -undefined dynamic_lookup
else
  LDFLAGS += -shared -lpthread -ldl -lm
endif

all: $(OUTPUT)

$(OUTPUT): $(SRC) Makefile
	@mkdir -p $(PRIV_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRC)

clean:
	rm -f $(OUTPUT)

.PHONY: all clean
