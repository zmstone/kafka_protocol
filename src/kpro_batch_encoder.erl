%%%   Copyright (c) 2020, Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%% This module enables batch(v2) incremental encoding.
-module(kpro_batch_encoder).

-export([new/0, append/2, done/4]).
-export([count/1, bytes/1, data/1]).
-export_type([batch/0]).

-type seqno() :: kpro:seqno().
-type txn_ctx() :: kpro:txn_ctx().
-type msg_input() :: kpro:msg_input().
-type msg_ts() :: kpro:msg_ts().
-type compress_option() :: kpro:compress_option().
-type offset() :: kpro:offset().

-record(batch,
        { offset = 0 :: non_neg_integer() %% relative offset in batch
        , bytes = 0 :: non_neg_integer() %% accumulated bytes in batch
        , ts_base = 0 :: msg_ts() %% timestamp of the first message
        , ts_max = 0 :: msg_ts() %% max timestamp in batch
        , data = [] :: iodata()
        }).

-opaque batch() :: #batch{}.

%% @doc Return the number of collected messages.
-spec count(batch()) -> non_neg_integer().
count(#batch{offset = C}) -> C.

%% @doc Return the number of collected bytes.
-spec bytes(batch()) -> non_neg_integer().
bytes(#batch{bytes = B}) -> B.

%% @doc Retrun the encoded IoData.
-spec data(batch()) -> iodata().
data(#batch{data = D}) -> D.

%% @doc Create a new batch.
-spec new() -> batch().
new() -> #batch{}.

%% @doc Append one message to batch.
-spec append(batch(), msg_input() | [msg_input()]) -> batch().
append(Batch, []) -> Batch;
append(Batch, [Msg | Rest]) ->
  append(append(Batch, Msg), Rest);
append(Batch0, Msg) when is_map(Msg) ->
  Batch = ensure_ts_base(Batch0, Msg),
  #batch{ offset = Offset
        , bytes = Bytes
        , ts_base = TsBase
        , ts_max = TsMax
        , data = Data
        } = Batch,
  Ts = maps:get(ts, Msg, TsBase),
  Body = enc_record(Offset, Ts - TsBase, Msg),
  Size = iolist_size(Body),
  Batch#batch{ offset = Offset + 1
             , bytes = Bytes + Size
             , data = [Data | [enc(varint, Size) | Body]]
             , ts_max = erlang:max(Ts, TsMax)
             }.

%% @doc Done collecting one batch, wrap it up.
-spec done(batch(), compress_option(), seqno(), txn_ctx()) -> iodata().
done(B, Compression, FirstSequence,
     #{ producer_id := ProducerId
      , producer_epoch := ProducerEpoch
      }) ->
  IsTxn = is_integer(ProducerId) andalso ProducerId >= 0,
  EncodedAttributes = encode_attributes(Compression, IsTxn),
  PartitionLeaderEpoch = -1, % producer can set whatever
  FirstOffset = 0, % always 0
  Magic = 2, % always 2
  LastOffsetDelta = B#batch.offset - 1,
  Body0 =
    [ EncodedAttributes           % {Attributes0,     T1} = dec(int16, T0),
    , enc(int32, LastOffsetDelta) % {LastOffsetDelta, T2} = dec(int32, T1),
    , enc(int64, B#batch.ts_base) % {FirstTimestamp,  T3} = dec(int64, T2),
    , enc(int64, B#batch.ts_max)  % {MaxTimestamp,    T4} = dec(int64, T3),
    , enc(int64, ProducerId)      % {ProducerId,      T5} = dec(int64, T4),
    , enc(int16, ProducerEpoch)   % {ProducerEpoch,   T6} = dec(int16, T5),
    , enc(int32, FirstSequence)   % {FirstSequence,   T7} = dec(int32, T6),
    , enc(int32, B#batch.offset)  % {Count,           T8} = dec(int32, T7),
    , kpro_compress:compress(Compression, B#batch.data)
    ],
  CRC = crc32cer:nif(Body0),
  Body =
    [ enc(int32, PartitionLeaderEpoch)
    , enc(int8,  Magic)
    , enc(int32, CRC)
    | Body0
    ],
  Size = iolist_size(Body),
  [ enc(int64, FirstOffset)
  , enc(int32, Size)
  | Body
  ].

%%%_* Internals ================================================================

%% Make sure ts_base is initialized when the first message is appended.
ensure_ts_base(#batch{offset = 0} = Batch, Msg) ->
  TsBase = case maps:get(ts, Msg, false) of
             false -> kpro_lib:now_ts();
             Ts -> Ts
           end,
  Batch#batch{ts_base = TsBase};
ensure_ts_base(B, _) -> B.

% Record =>
%   Length => varint
%   Attributes => int8
%   TimestampDelta => varint
%   OffsetDelta => varint
%   KeyLen => varint
%   Key => data
%   ValueLen => varint
%   Value => data
%   Headers => [Header]
-spec enc_record(offset(), msg_ts(), msg_input()) -> iodata().
enc_record(Offset, TsDelta, #{value := Value} = M) ->
  Key = maps:get(key, M, <<>>),
  %% 'headers' is a non-nullable array
  %% do not encode 'undefined' -> -1
  Headers = maps:get(headers, M, []),
  [ enc(int8, 0) % no per-message attributes in magic v2
  , enc(varint, TsDelta)
  , enc(varint, Offset)
  , enc(bytes, Key)
  , enc(bytes, Value)
  | enc_headers(Headers)
  ].

enc_headers(Headers) ->
  Count = length(Headers),
  [ enc(varint, Count)
  | [enc_header(Header) || Header <- Headers]
  ].

% Header => HeaderKey HeaderVal
%   HeaderKeyLen => varint
%   HeaderKey => string
%   HeaderValueLen => varint
%   HeaderValue => data
enc_header({Key, Val}) ->
  [ enc(varint, size(Key))
  , Key
  , enc(varint, size(Val))
  , Val
  ].

-spec encode_attributes(compress_option(), boolean()) -> iolist().
encode_attributes(Compression, IsTxn0) ->
  Codec = kpro_compress:method_to_codec(Compression),
  TsType = 0, % producer always set 0
  IsTxn = flag(IsTxn0, 1 bsl 4),
  IsCtrl = flag(false, 1 bsl 5),
  Result = Codec bor TsType bor IsTxn bor IsCtrl,
  %% yes, it's int16 for batch level attributes
  %% message level attributes (int8) are currently unused in magic v2
  %% and maybe get used in future magic versions
  enc(int16, Result).

flag(false, _) -> 0;
flag(true, BitMask) -> BitMask.

enc(bytes, undefined) ->
  enc(varint, -1);
enc(bytes, <<>>) ->
  enc(varint, -1);
enc(bytes, Bin) ->
  Len = size(Bin),
  [enc(varint, Len), Bin];
enc(Primitive, Val) ->
  kpro_lib:encode(Primitive, Val).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
