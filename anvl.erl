-include("anvl.hrl").

conf() ->
  #{ plugins => [anvl_git, anvl_hex_pm, anvl_rebar3, anvl_erlc, anvl_texinfo]
   , conditions => [all, docs, static_checks, compile]
   , erlang =>
       #{ bdeps => [proper, familiar]
        , compile =>
            #{ options => []
             , global_z => [debug_info]
             }
        , static_checks =>
            #{ apps => [classy]
             , non_runtime_deps => [proper, familiar]
             }
        }
   , deps =>
       #{ git =>
            [ #{id => hackney,    repo => "https://github.com/emqx/hackney.git",      ref => {tag,"1.18.1-1"}}
            , #{id => eetcd,      repo => "https://github.com/zhongwencool/eetcd",    ref => {tag,"v0.6.0"}}
            , #{id => optvar,     repo => "https://github.com/emqx/optvar",           ref => {tag,"1.0.5"}}
            , #{id => snabbkaffe, repo => "https://github.com/kafka4beam/snabbkaffe", ref => {tag,"1.0.10"}}
            , #{id => gproc,      repo => "https://github.com/uwiger/gproc",          ref => {tag,"1.1.0"}}
            , #{id => familiar,   repo => "https://github.com/ieQu1/familiar",        ref => {tag,"0.1.2"}}
            ]
        , hex_pm =>
            [ #{id => proper, version => "1.5.0"}
            , #{id => jsone,  version => "1.9.0"}
            ]
        , local =>
            [ #{kind => otp_application, dir => "_checkouts/*"}
            ]
        }
   , texinfo =>
       #{ sources => ["doc/classy.texi"]
        , formats => [html, info]
        , compile =>
            [#{ format => html
              , options => ["-c", "INFO_JS_DIR=js"]
              }
            ]
        }
   }.

?MEMO(all,
      precondition([static_checks(), docs()])).

compile() ->
  anvl_erlc:app_compiled(default, classy).

?MEMO(static_checks,
      precondition(
        [ anvl_erlc_dialyzer:passed(default)
        , anvl_erlc_xref:passed(default)
        ])).

?MEMO(docs,
      begin
        precondition(anvl_texinfo:erl_doc(default, classy)) or
          precondition(anvl_texinfo:compiled(anvl_project:root()))
      end).
