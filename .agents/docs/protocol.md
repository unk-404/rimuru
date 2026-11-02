> 本文保留原始设计细节；如与 [版本和现状](versioning.md) 冲突，以后者和当前源码为准。历史测量归档保存在本机忽略目录，不构成当前验收。

## 4. 协议 0.0 线格式（历史设计主体）

本节固定字段与状态语义。多字节整数均为小端；不使用varint。固定字段消除解析分支，压缩靠复用标识、批量范围和有界状态完成。未知版本拒绝，未知可选扩展丢弃并计数，未知mandatory扩展终止会话。

### 4.1 公共头与包类型

所有报文公共头为13B：`type_flags:u8 @0`、`sender_incarnation:u64 @1`、`frame_id:u32 @9`。首字节高4位为type，低4位flags；flags仅bit0为ACK_ELICITING，其余保留位必须为0。DATA16/32分别追加`frag_idx:u16 @13`或`u32 @13`，因此头为15/17B，随后直接接payload，无DATA尾部。DATA类型自身携带索引宽度，不再依赖丢失BEGIN猜头。

| type | 名称 | frame_id与含义 |
|---:|---|---|
| 0 | DATA16 | 非0帧ID，u16片索引 |
| 1 | DATA32 | 非0帧ID，u32片索引 |
| 2 | HELLO | 0，新握手 |
| 3 | RETRY | 0，无状态来源挑战 |
| 4 | HELLO_ACK | 0，选择参数及服务端挑战 |
| 5 | CONFIRM | 0，确认双向身份 |
| 6 | BEGIN | 非0，声明并申请一条帧/消息 |
| 7 | BEGIN_ACCEPT | 非0，幂等消息授权 |
| 8 | ACK | 0，批量报告片段 |
| 9 | CREDIT | 0，资源快照，只是提示 |
| 10 | NACK | 非0，缺口范围 |
| 11 | COMPLETE | 非0，校验/交付责任确认及其确认 |
| 12 | ABORT | 非0表示消息，0由scope区分channel/会话 |
| 13 | ABORT_ACK | 对应ABORT |
| 14 | PING | 0或目标帧，触发存活/完成探测 |
| 15 | EXT | `subtype:u16,body_len:u16,body[body_len]` |

除DATA外，所有报文尾附`control_crc32c:u32`，覆盖公共头与整个body。EXT普通控制报文不超过协商`max_control_bytes`，FEC_REPAIR属于数据类、受D/PMTU限制而不受小控制包上限。EXT subtype固定：0=PONG、1=ERROR、2=FEC_REPAIR、3=CLOSE、4=CLOSE_ACK；高位0x8000表示mandatory。不认识的type不可能绕过4bit表；保留flags/错误长度均丢弃并计数。

DATA16/32可按每条消息选择，BEGIN的idx_bytes必须与其一致；≤65536段用16位，否则32位。普通DATA的ACK_ELICITING在L2/L3为1，在L1为0。COMPLETE的bit0=1表示接收方请求终态确认，bit0=0表示发送方确认该终态；其他控制帧flags=0。L2/L3不增加普通DATA字段。

`sender_incarnation`是每方向新鲜非零u64，由OS随机源播种的全进程单调分配器产生；进程存活期间不复用，耗尽时拒绝新Session而不回绕，避免为历史身份保存无界集合。接收同时校验来源地址与已确认方向身份。跨进程重启的64位碰撞/主动伪造不属于密码学保证；旧身份包不属于当前Session。不得降成1B epoch或在“只有一个peer”时省略。v1不采用DATA_T/I、packed channel、聚合/私有train头；这些进入后续协议版本，不留首版待决开关。

### 4.2 握手与控制body

`magic[4]`固定为ASCII `RMRU`，wire major/minor为0/0。HELLO body按顺序：`magic[4],major:u16,minor:u16,client_nonce[16],cookie[32],caps:u64,max_message:u32,max_control:u16,max_d:u16,integrity:u8,reserved[3]`。首次cookie全0。所有reserved为0；cap bit0=CRC32C、bit1=UDP_ONLY、bit2=RS64、bit3=L2可靠消息、bit4=L3有序channel；其余未知位忽略，选用交集。bit3/bit4自动协商，不提供Cargo功能开关；L3必须同时协商L2。未协商时相应Rust入口返回ReliableUnsupported/OrderedUnsupported，仍可使用已协商的下层。当前不通告RS64。

RETRY body为`client_nonce[16],cookie[32]`，公共头incarnation=0；cookie采用BLAKE3 keyed hash，输入为规范化源地址/端口、client incarnation、client_nonce、30秒时间桶，接受当前/前一桶；秘密每个Runtime随机生成并保留当前/前一轮换秘密。此MAC只验证握手来源、防放大，不加密数据、不提供peer身份认证。RETRY不超过触发HELLO长度，未验证来源不分配帧缓冲 [S28]。

HELLO_ACK body为`client_nonce[16],server_nonce[16],caps:u64,max_message:u32,max_control:u16,max_d:u16,integrity:u8,reserved[3]`，公共头使用服务端方向身份。CONFIRM body为`client_nonce[16],server_nonce[16],peer_incarnation:u64`，客户端首先发送，公共头为自己的身份、body回显服务端身份。服务端验证后以相同body形状回应CONFIRM，公共头为服务端身份、body回显客户端身份；客户端收到有效回应才发应用数据，服务端收到客户端CONFIRM后可接受其数据。CONFIRM丢失/重复时重发对应回应。双nonce、身份与地址均匹配才启用数据上下文；旧握手不覆盖活跃会话，未验证peer临时状态最多1KiB，全局限额见第13节。

max_message取双方策略较小值；max_control取双方上限与D之较小值，最小128B；max_d为UDP payload上限，不超过65507且须通过路径探测。默认integrity=1即全帧CRC；2=UDP_ONLY仅在双方显式允许的受信策略下协商。无none。相同地址的双向同时HELLO由`(incarnation,nonce)`字典序较小者作为发起者合并，另一方保留自己的方向身份，避免生成两套互相覆盖的Session。

| 控制body | 固定布局/处理 |
|---|---|
| BEGIN | `layer:u8,idx_bytes:u8,integrity:u8,meta_flags:u8,channel:u16,reserved:u16,total_len:u32,stride:u32,count:u32,channel_seq:u32,frame_crc32c:u32,fec_code:u8,reserved:u8,fec_k:u16,fec_r_max:u16,reserved:u16`，共36B |
| BEGIN_ACCEPT | `grant_id:u64,charged_bytes:u64,credit_seq:u64`；绑定当前frame_id/BEGIN；重复请求返回同授权，不多收费 |
| ACK | `ack_delay_us:u32,record_count:u16,reserved:u16`，随后每记录`frame_id:u32,range_count:u16,reserved:u16`及`(start:u32,len:u32)`数组；一次报文不能超过控制上限 |
| CREDIT | `credit_seq:u64,available_bytes:u64,available_messages:u32,available_events:u32`；旧seq忽略；不能据快照直接发送未获grant的L2 DATA |
| NACK | `range_count:u16,reserved:u16`及`(start:u32,len:u32)`；上下文不存在/已终止时不分配新资源 |
| COMPLETE | 空body；仅接收方完成长度/CRC检查并承担交付责任后发送；ACK_ELICITING区别请求与确认 |
| ABORT/ABORT_ACK | `reason:u16,scope:u8,target_direction:u8,channel:u16,reserved:u16,last_channel_seq:u64`，共16B；scope=0消息、1channel、2session；direction=0发起者→响应者、1反向；应答回显终态；last_channel_seq表示目标方向已按序发布到接收队列的末序号，无已发布前缀时为UINT64_MAX；scope=2尚未实现 |
| PING / EXT PONG | `nonce:u64`；带非0frame_id的PING要求重发该帧可用的ACK/COMPLETE/ABORT终态 |
| EXT ERROR | `status:u16,detail:u16`；不反射任意文本 |
| EXT CLOSE/CLOSE_ACK | `reason:u16,reserved:u16`；可靠终止会话，必要时重复 |

BEGIN的layer取1/2/3；meta_flags bit0表示显式channel_seq，其余0。channel字段始终存在；L1/L2的seq填0、bit0=0，L3始终bit0=1并携带本channel连续seq。这样动态增加其他channel也不会让原有顺序键改变。字段已在公共BEGIN中占位，所以L3相对同布局L2不额外增加DATA或BEGIN字节；不再虚称省略了实际存在的字段。可靠消息BEGIN→授权→DATA；可并行申请多条，控制报文可在一次系统调用中批量发送，但不把多个控制报文拼成未经定义的单报文。

DATA/BEGIN/FEC的frame_id属于发送者方向；ACK/NACK/BEGIN_ACCEPT针对对端发送方向；COMPLETE请求针对对端发送、确认针对自己的发送；带帧PING查询自己的发送消息。ABORT显式target_direction消除双向相同frame_id歧义，scope=channel终止指定发送方向，双方清理该方向对应状态，对端反向同ID channel独立；scope=session仍为后续规范。wire reason固定0 NORMAL、1 RESOURCE、2 INTEGRITY、3 PROTOCOL、4 TIMEOUT、5 CANCELLED、6 MTU、7 SHUTDOWN、8 ID_EXHAUSTED；ERROR使用wire码及detail，不截断C负状态码。

CRC计算固定为CRC32C(Castagnoli)，标准初值/终异或采用`crc32c` crate接口定义；校验向量ASCII `123456789`应为0xE3069283。帧CRC覆盖按小端编码的`frame_id,layer,idx_bytes,integrity,meta_flags,channel,total_len,stride,count,channel_seq,fec_code,fec_k,fec_r_max`，随后为恰好total_len字节payload；排除CRC字段、reserved与发送时刻。BEGIN中CRC字段在UDP_ONLY为0。控制CRC始终存在，检测错误控制状态。帧CRC失败发ABORT(INTEGRITY)，不发成功COMPLETE；发送保留到成功COMPLETE或明确终止。

### 4.3 长度、ID、去重与完成

非空消息要求stride>0，`count=total_len/stride+(total_len%stride!=0)`；所有乘加先checked u64计算，再检查目标地址空间/预算。片索引范围`[0,count)`，非尾片长度严格为stride、尾片恰为剩余字节；截断/越界/不一致BEGIN拒绝。空消息count=0，由BEGIN经完整性校验完成；可靠层仍需授权和终态。相同frame_id的BEGIN所有语义字段必须一致。

两个方向独立取号，frame_id从1至u32::MAX、channel_seq从0至u32::MAX；不在同方向incarnation内复用。耗尽前停止准入，排空/终止后重握手。并发发送的channel_seq/frame_id在成功准入线性化点分配，失败前不留洞。协议支持u32总长，实际超策略或地址空间一律明确拒绝。

L2/L3接收方ACK Ready片后保持其缓冲；L2全部片到齐并CRC成功后发布到已预留的接收队列，再发送COMPLETE(bit0=1)。L3完整校验后先进入Ready重排状态；被前驱阻塞的后继独立SACK全部片，可立即按序发布的消息直接用COMPLETE确认。只有连续前缀按序发布到同一接收队列后才发送COMPLETE(bit0=1)。发送方收到后可释放可靠性保留、回COMPLETE(bit0=0)；内核ZC引用仍独立等待。任意一方重试/重复不重复交付。COMPLETE丢失时用PING/重复BEGIN恢复终态，不能把“全部片ACK”当成功。

去重账本以已结束ID区间+活跃例外存储；每Session默认最多4096个区间，计入硬预算。墓碑不按墙钟TTL淘汰后重新接收同incarnation旧ID；区间过多或ID耗尽则排空并重握手。已确认结束的旧BEGIN/DATA不得重建消息。当前Session以外身份一律拒绝，退役记录仅用于诊断/握手退避，保留120秒并限额4096，满则最旧诊断项可淘汰，不能影响当前身份判断。

ABORT与COMPLETE在owner上只允许一个终态胜出。已Complete/已入交付队列时取消返回ALREADY_COMPLETED；先Abort则不再交付，并回ABORT_ACK。无法取得远端终态时本地票据为OUTCOME_UNKNOWN。L3前驱失败终止该channel，已交付前缀保留、未交付后继终止；不隐式跳号。关闭/资源失败遵循同一终态规则。

### 4.4 FEC、分段与PMTU

FEC关闭为默认，协商fec_code=1时采用`reed-solomon-simd`的系统码格式，独立块最多k=64源片、固定r_max=32修复片；末块k取实际源片数。BEGIN携带code/k/r_max，双方据相同计数生成同一编码参数。EXT FEC_REPAIR body为`block_index:u32,repair_index:u16,symbol_bytes:u16,source_count:u16,reserved:u16,symbol[symbol_bytes]`。repair_index范围0..31，不混占DATA frag_idx；恢复片映射回原frame_id/frag_idx，经同一发布/去重/CRC入口。

symbol_bytes固定为偶数stride，尾片内部补0仅供编码，线上普通DATA不补齐。repair报文含13B头+4B扩展头+12B符号头+symbol+4B控制CRC，故FEC模式stride不超过`(D-33)&~1`；不用source DATA的15/17B长度直接计算repair上限。冗余发送量由实测p/burst及预算决定，起始为0，最大32；编码、scratch和块等待均记账。v1不同时引入喷泉码/滑窗码，后续需新fec_code。FEC开启的datagram长度分布必须在raw对照中匹配 [S26]。

D是分段后UDP payload上限，stride是每片应用容量，train是一次提交的多datagram。GSO一次train各满段尺寸固定、尾段可短，每个切出段都有自己的公共头；无GSO回退批量/逐段。GRO/coalescing必须通过ancillary还原边界；缺失/截断回退并计数。禁止在train前插私有run header。

默认bootstrap D=min(1200,已知路径安全上限)，探测可降低；不是声称任意路径都能安全承载1200。真实网络避免IP分片；65507只作适用IPv4/loopback边界实验。帧内stride固定，PMTU下降无法完成时明确终止当前消息，新消息用新stride；v1不做原ID透明重分片。单socket内探测，不开额外UDP socket。

