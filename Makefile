PRIV_DIR = $(MIX_APP_PATH)/priv
OUTPUT = $(PRIV_DIR)/ekv_sqlite3_nif.so

ERL_INCLUDE := $(ERTS_INCLUDE_DIR)
ifeq ($(ERL_INCLUDE),)
  ERL_INCLUDE := $(shell erl -noshell -eval 'io:put_chars(filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "include"])), halt().')
endif

SRC = c_src/ekv_sqlite3_nif.c c_src/sqlite3.c

CFLAGS += -O2 -fPIC -I$(ERL_INCLUDE) -Ic_src \
  -DSQLITE_THREADSAFE=1 \
  -DSQLITE_USE_URI=1 \
  -DSQLITE_DQS=0 \
  -DHAVE_USLEEP=1 \
  -DSQLITE_ENABLE_STAT4=1 \
  -DSQLITE_LIKE_DOESNT_MATCH_BLOBS=1 \
  -DNDEBUG=1

# Opt-in only: the runner uses a separate Mix build tree and preloads ASan.
ifneq ($(SANITIZE),)
  CFLAGS += -O1 -g -fno-omit-frame-pointer -fsanitize=$(SANITIZE) -fno-sanitize-recover=all
  LDFLAGS += -fsanitize=$(SANITIZE)
endif

CLANG ?= clang
C_WARNINGS = -Wall -Wextra -Wformat=2 -Wshadow -Werror

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

$(OUTPUT): $(SRC) c_src/sqlite3.h Makefile
	@mkdir -p $(PRIV_DIR)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(SRC)

# Keep strict diagnostics on our wrapper, not the vendored amalgamation.
# analyzer-werror is separate from -Werror: findings must fail CI too.
c-check:
	$(CLANG) $(CFLAGS) $(C_WARNINGS) -fsyntax-only c_src/ekv_sqlite3_nif.c
	$(CLANG) $(CFLAGS) $(C_WARNINGS) --analyze \
	  -Xanalyzer -analyzer-output=text -Xanalyzer -analyzer-werror \
	  c_src/ekv_sqlite3_nif.c

clean:
	rm -f $(OUTPUT)

.PHONY: all clean c-check
