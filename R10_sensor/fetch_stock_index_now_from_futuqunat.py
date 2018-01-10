"""
得到一个指数下面所有的股票信息
"""
from futuquant.open_context import *
from R50_general.general_constants import futu_api_ip as api_ip
from R50_general.general_constants import futu_api_port as api_port
import pandas as pd
from pandas import Series, DataFrame
import numpy as np

import re

from datetime import datetime

from R50_general.DBconnectionmanager import Dbconnectionmanager as dcm
from R50_general.general_helper_funcs import logprint
import R50_general.dfm_to_table_common as df2db
import R50_general.advanced_helper_funcs as ahf
import R50_general.general_constants
import R50_general.general_helper_funcs as gcf

global_module_name = 'fetch_stock_index_now_from_futuquant'

# 如下数据源由 enum_all_index_stocks 接口生成, 后续请自行手动运行更新
'''SH INDEX
 code name
0 SH.000000 060001
1 SH.000001 上证指数
2 SH.000002 A股指数
3 SH.000003 B股指数
4 SH.000004 工业指数
5 SH.000005 商业指数
6 SH.000006 地产指数
7 SH.000007 公用指数
8 SH.000008 综合指数
9 SH.000009 上证380
10 SH.000010 上证180
11 SH.000011 基金指数
12 SH.000012 国债指数
13 SH.000013 企债指数
14 SH.000015 红利指数
15 SH.000016 上证50
16 SH.000017 新综指
17 SH.000018 180金融
18 SH.000019 治理指数
19 SH.000020 中型综指
20 SH.000021 180治理
21 SH.000022 沪公司债
22 SH.000023 沪分离债
23 SH.000025 180基建
24 SH.000026 180资源
25 SH.000027 180运输
26 SH.000028 180成长
27 SH.000029 180价值
28 SH.000030 180R成长
29 SH.000031 180R价值
30 SH.000032 上证能源
31 SH.000033 上证材料
32 SH.000034 上证工业
33 SH.000035 上证可选
34 SH.000036 上证消费
35 SH.000037 上证医药
36 SH.000038 上证金融
37 SH.000039 上证信息
38 SH.000040 上证电信
39 SH.000041 上证公用
40 SH.000042 上证央企
41 SH.000043 超大盘
42 SH.000044 上证中盘
43 SH.000045 上证小盘
44 SH.000046 上证中小
45 SH.000047 上证全指
46 SH.000048 责任指数
47 SH.000049 上证民企
48 SH.000050 50等权
49 SH.000051 180等权
50 SH.000052 50基本
51 SH.000053 180基本
52 SH.000054 上证海外
53 SH.000055 上证地企
54 SH.000056 上证国企
55 SH.000057 全指成长
56 SH.000058 全指价值
57 SH.000059 全R成长
58 SH.000060 全R价值
59 SH.000061 沪企债30
60 SH.000062 上证沪企
61 SH.000063 上证周期
62 SH.000064 非周期
63 SH.000065 上证龙头
64 SH.000066 上证商品
65 SH.000067 上证新兴
66 SH.000068 上证资源
67 SH.000069 消费80
68 SH.000070 能源等权
69 SH.000071 材料等权
70 SH.000072 工业等权
71 SH.000073 可选等权
72 SH.000074 消费等权
73 SH.000075 医药等权
74 SH.000076 金融等权
75 SH.000077 信息等权
76 SH.000078 电信等权
77 SH.000079 公用等权
78 SH.000090 上证流通
79 SH.000091 沪财中小
80 SH.000092 资源50
81 SH.000093 180分层
82 SH.000094 上证上游
83 SH.000095 上证中游
84 SH.000096 上证下游
85 SH.000097 高端装备
86 SH.000098 上证F200
87 SH.000099 上证F300
88 SH.000100 上证F500
89 SH.000101 5年信用
90 SH.000102 沪投资品
91 SH.000103 沪消费品
92 SH.000104 380能源
93 SH.000105 380材料
94 SH.000106 380工业
95 SH.000107 380可选
96 SH.000108 380消费
97 SH.000109 380医药
98 SH.000110 380金融
99 SH.000111 380信息
100 SH.000112 380电信
101 SH.000113 380公用
102 SH.000114 持续产业
103 SH.000115 380等权
104 SH.000116 信用100
105 SH.000117 380成长
106 SH.000118 380价值
107 SH.000119 380R成长
108 SH.000120 380R价值
109 SH.000121 医药主题
110 SH.000122 农业主题
111 SH.000123 180动态
112 SH.000125 180稳定
113 SH.000126 消费50
114 SH.000128 380基本
115 SH.000129 180波动
116 SH.000130 380波动
117 SH.000131 上证高新
118 SH.000132 上证100
119 SH.000133 上证150
120 SH.000134 上证银行
121 SH.000135 180高贝
122 SH.000136 180低贝
123 SH.000137 380高贝
124 SH.000138 380低贝
125 SH.000139 上证转债
126 SH.000141 380动态
127 SH.000142 380稳定
128 SH.000145 优势资源
129 SH.000146 优势制造
130 SH.000147 优势消费
131 SH.000148 消费领先
132 SH.000149 180红利
133 SH.000150 380红利
134 SH.000151 上国红利
135 SH.000152 上央红利
136 SH.000153 上民红利
137 SH.000155 市值百强
138 SH.000158 上证环保
139 SH.000159 沪股通
140 SH.000160 沪新丝路
141 SH.000161 沪中国造
142 SH.000162 沪互联+
143 SH.000170 50AH优选
144 SH.000171 新兴成指
145 SH.000188 中国波指
146 SH.000300 沪深300
147 SH.000801 资源80
148 SH.000802 500沪市
149 SH.000803 300波动
150 SH.000804 500波动
151 SH.000805 A股资源
152 SH.000806 消费服务
153 SH.000807 食品饮料
154 SH.000808 医药生物
155 SH.000809 细分农业
156 SH.000810 细分能源
157 SH.000811 细分有色
158 SH.000812 细分机械
159 SH.000813 细分化工
160 SH.000814 细分医药
161 SH.000815 细分食品
162 SH.000816 细分地产
163 SH.000817 兴证海峡
164 SH.000818 细分金融
165 SH.000819 有色金属
166 SH.000820 煤炭指数
167 SH.000821 300红利
168 SH.000822 500红利
169 SH.000823 800有色
170 SH.000824 国企红利
171 SH.000825 央企红利
172 SH.000826 民企红利
173 SH.000827 中证环保
174 SH.000828 300高贝
175 SH.000829 300低贝
176 SH.000830 500高贝
177 SH.000831 500低贝
178 SH.000832 中证转债
179 SH.000833 中高企债
180 SH.000838 创业价值
181 SH.000839 浙企综指
182 SH.000840 浙江民企
183 SH.000841 800医药
184 SH.000842 800等权
185 SH.000843 300动态
186 SH.000844 300稳定
187 SH.000846 ESG100
188 SH.000847 腾讯济安
189 SH.000849 300非银
190 SH.000850 300有色
191 SH.000851 百发100
192 SH.000852 中证1000
193 SH.000853 CSSW丝路
194 SH.000854 500原料
195 SH.000855 央视500
196 SH.000856 500工业
197 SH.000857 500医药
198 SH.000858 500信息
199 SH.000863 CS精准医
200 SH.000865 上海国企
201 SH.000867 港中小企
202 SH.000869 HK银行
203 SH.000874 000874
204 SH.000891 新兴综指
205 SH.000901 小康指数
206 SH.000902 中证流通
207 SH.000903 中证100
208 SH.000904 中证200
209 SH.000905 中证500
210 SH.000906 中证800
211 SH.000907 中证700
212 SH.000908 300能源
213 SH.000909 300材料
214 SH.000910 300工业
215 SH.000911 300可选
216 SH.000912 300消费
217 SH.000913 300医药
218 SH.000914 300金融
219 SH.000915 300信息
220 SH.000916 300电信
221 SH.000917 300公用
222 SH.000918 300成长
223 SH.000919 300价值
224 SH.000920 300R成长
225 SH.000921 300R价值
226 SH.000922 中证红利
227 SH.000923 公司债
228 SH.000924 分离债
229 SH.000925 基本面50
230 SH.000926 中证央企
231 SH.000927 央企100
232 SH.000928 中证能源
233 SH.000929 中证材料
234 SH.000930 中证工业
235 SH.000931 中证可选
236 SH.000932 中证消费
237 SH.000933 中证医药
238 SH.000934 中证金融
239 SH.000935 中证信息
240 SH.000936 中证电信
241 SH.000937 中证公用
242 SH.000938 中证民企
243 SH.000939 民企200
244 SH.000940 财富大盘
245 SH.000941 新能源
246 SH.000942 内地消费
247 SH.000943 内地基建
248 SH.000944 内地资源
249 SH.000945 内地运输
250 SH.000946 内地金融
251 SH.000947 内地银行
252 SH.000948 内地地产
253 SH.000949 内地农业
254 SH.000950 300基建
255 SH.000951 300银行
256 SH.000952 300地产
257 SH.000953 中证地企
258 SH.000954 地企100
259 SH.000955 中证国企
260 SH.000956 国企200
261 SH.000957 300运输
262 SH.000958 创业成长
263 SH.000959 银河99
264 SH.000960 中证龙头
265 SH.000961 中证上游
266 SH.000962 中证中游
267 SH.000963 中证下游
268 SH.000964 中证新兴
269 SH.000965 基本200
270 SH.000966 基本400
271 SH.000967 基本600
272 SH.000968 300周期
273 SH.000969 300非周
274 SH.000970 ESG40
275 SH.000971 等权90
276 SH.000972 300沪市
277 SH.000973 技术领先
278 SH.000974 800金融
279 SH.000975 钱江30
280 SH.000976 新华金牛
281 SH.000977 内地低碳
282 SH.000978 医药100
283 SH.000979 大宗商品
284 SH.000980 中证超大
285 SH.000981 300分层
286 SH.000982 500等权
287 SH.000983 智能资产
288 SH.000984 300等权
289 SH.000985 中证全指
290 SH.000986 全指能源
291 SH.000987 全指材料
292 SH.000988 全指工业
293 SH.000989 全指可选
294 SH.000990 全指消费
295 SH.000991 全指医药
296 SH.000992 全指金融
297 SH.000993 全指信息
298 SH.000994 全指电信
299 SH.000995 全指公用
300 SH.000996 领先行业
301 SH.000997 大消费
302 SH.000998 中证TMT
303 SH.000999 两岸三地
'''

'''SZ INDEX
0 SZ.399001 深证成指
1 SZ.399002 深成指R
2 SZ.399003 成份B指
3 SZ.399004 深证100R
4 SZ.399005 中小板指
5 SZ.399006 创业板指
6 SZ.399007 深证300
7 SZ.399008 中小300
8 SZ.399009 深证200
9 SZ.399010 深证700
10 SZ.399011 深证1000
11 SZ.399012 创业300
12 SZ.399013 深市精选
13 SZ.399015 中小创新
14 SZ.399016 深证创新
15 SZ.399017 SME创新
16 SZ.399018 创业创新
17 SZ.399100 新指数
18 SZ.399101 中小板综
19 SZ.399102 创业板综
20 SZ.399103 乐富指数
21 SZ.399106 深证综指
22 SZ.399107 深证A指
23 SZ.399108 深证B指
24 SZ.399231 农林指数
25 SZ.399232 采矿指数
26 SZ.399233 制造指数
27 SZ.399234 水电指数
28 SZ.399235 建筑指数
29 SZ.399236 批零指数
30 SZ.399237 运输指数
31 SZ.399238 餐饮指数
32 SZ.399239 IT指数
33 SZ.399240 金融指数
34 SZ.399241 地产指数
35 SZ.399242 商务指数
36 SZ.399243 科研指数
37 SZ.399244 公共指数
38 SZ.399248 文化指数
39 SZ.399249 综企指数
40 SZ.399298 深信中高
41 SZ.399299 深信中低
42 SZ.399300 沪深300
43 SZ.399301 深信用债
44 SZ.399302 深公司债
45 SZ.399303 国证2000
46 SZ.399305 基金指数
47 SZ.399306 深证ETF
48 SZ.399307 深证转债
49 SZ.399310 国证50
50 SZ.399311 国证1000
51 SZ.399312 国证300
52 SZ.399313 巨潮100
53 SZ.399314 巨潮大盘
54 SZ.399315 巨潮中盘
55 SZ.399316 巨潮小盘
56 SZ.399317 国证A指
57 SZ.399318 国证B指
58 SZ.399319 资源优势
59 SZ.399320 国证服务
60 SZ.399321 国证红利
61 SZ.399322 国证治理
62 SZ.399324 深证红利
63 SZ.399326 成长40
64 SZ.399328 深证治理
65 SZ.399330 深证100
66 SZ.399332 深证创新
67 SZ.399333 中小板R
68 SZ.399335 深证央企
69 SZ.399337 深证民营
70 SZ.399339 深证科技
71 SZ.399341 深证责任
72 SZ.399344 深证300R
73 SZ.399346 深证成长
74 SZ.399348 深证价值
75 SZ.399350 皖江30
76 SZ.399351 深报指数
77 SZ.399352 深报综指
78 SZ.399353 国证物流
79 SZ.399354 分析师指数
80 SZ.399355 长三角
81 SZ.399356 珠三角
82 SZ.399357 环渤海
83 SZ.399358 泰达指数
84 SZ.399359 国证基建
85 SZ.399360 新硬件
86 SZ.399361 国证商业
87 SZ.399362 国证民营
88 SZ.399363 计算机指
89 SZ.399364 中金消费
90 SZ.399365 国证农业
91 SZ.399366 国证大宗
92 SZ.399367 巨潮地产
93 SZ.399368 国证军工
94 SZ.399369 CBN-兴全
95 SZ.399370 国证成长
96 SZ.399371 国证价值
97 SZ.399372 大盘成长
98 SZ.399373 大盘价值
99 SZ.399374 中盘成长
100 SZ.399375 中盘价值
101 SZ.399376 小盘成长
102 SZ.399377 小盘价值
103 SZ.399378 南方低碳
104 SZ.399379 国证基金
105 SZ.399380 国证ETF
106 SZ.399381 1000能源
107 SZ.399382 1000材料
108 SZ.399383 1000工业
109 SZ.399384 1000可选
110 SZ.399385 1000消费
111 SZ.399386 1000医药
112 SZ.399387 1000金融
113 SZ.399388 1000信息
114 SZ.399389 国证通信
115 SZ.399390 1000公用
116 SZ.399391 投资时钟
117 SZ.399392 国证新兴
118 SZ.399393 国证地产
119 SZ.399394 国证医药
120 SZ.399395 国证有色
121 SZ.399396 国证食品
122 SZ.399397 OCT文化
123 SZ.399398 绩效指数
124 SZ.399399 中经GDP
125 SZ.399400 大中盘
126 SZ.399401 中小盘
127 SZ.399402 周期100
128 SZ.399403 防御100
129 SZ.399404 大盘低波
130 SZ.399405 大盘高贝
131 SZ.399406 中盘低波
132 SZ.399407 中盘高贝
133 SZ.399408 小盘低波
134 SZ.399409 小盘高贝
135 SZ.399410 苏州率先
136 SZ.399411 红利100
137 SZ.399412 国证新能
138 SZ.399413 国证转债
139 SZ.399415 I100
140 SZ.399416 I300
141 SZ.399417 新能源车
142 SZ.399418 国证国安
143 SZ.399419 国证高铁
144 SZ.399420 国证保证
145 SZ.399422 中关村A
146 SZ.399423 中关村50
147 SZ.399427 专利领先
148 SZ.399428 国证定增
149 SZ.399429 新丝路
150 SZ.399431 国证银行
151 SZ.399432 国证汽车
152 SZ.399433 国证交运
153 SZ.399434 国证传媒
154 SZ.399435 国证农牧
155 SZ.399436 国证煤炭
156 SZ.399437 国证证券
157 SZ.399438 国证电力
158 SZ.399439 国证油气
159 SZ.399440 国证钢铁
160 SZ.399441 生物医药
161 SZ.399481 企债指数
162 SZ.399550 央视50
163 SZ.399551 央视创新
164 SZ.399552 央视成长
165 SZ.399553 央视回报
166 SZ.399554 央视治理
167 SZ.399555 央视责任
168 SZ.399556 央视生态
169 SZ.399557 央视文化
170 SZ.399602 中小成长
171 SZ.399604 中小价值
172 SZ.399606 创业板R
173 SZ.399608 科技100
174 SZ.399610 TMT50
175 SZ.399611 中创100R
176 SZ.399612 中创100
177 SZ.399613 深证能源
178 SZ.399614 深证材料
179 SZ.399615 深证工业
180 SZ.399616 深证可选
181 SZ.399617 深证消费
182 SZ.399618 深证医药
183 SZ.399619 深证金融
184 SZ.399620 深证信息
185 SZ.399621 深证电信
186 SZ.399622 深证公用
187 SZ.399623 中小基础
188 SZ.399624 中创400
189 SZ.399625 中创500
190 SZ.399626 中创成长
191 SZ.399627 中创价值
192 SZ.399628 700成长
193 SZ.399629 700价值
194 SZ.399630 1000成长
195 SZ.399631 1000价值
196 SZ.399632 深100EW
197 SZ.399633 深300EW
198 SZ.399634 中小板EW
199 SZ.399635 创业板EW
200 SZ.399636 深证装备
201 SZ.399637 深证地产
202 SZ.399638 深证环保
203 SZ.399639 深证大宗
204 SZ.399640 创业基础
205 SZ.399641 深证新兴
206 SZ.399642 中小新兴
207 SZ.399643 创业新兴
208 SZ.399644 深证时钟
209 SZ.399645 100低波
210 SZ.399646 深消费50
211 SZ.399647 深医药50
212 SZ.399648 深证GDP
213 SZ.399649 中小红利
214 SZ.399650 中小治理
215 SZ.399651 中小责任
216 SZ.399652 中创高新
217 SZ.399653 深证龙头
218 SZ.399654 深证文化
219 SZ.399655 深证绩效
220 SZ.399656 100绩效
221 SZ.399657 300绩效
222 SZ.399658 中小绩效
223 SZ.399659 深成指EW
224 SZ.399660 中创EW
225 SZ.399661 深证低波
226 SZ.399662 深证高贝
227 SZ.399663 中小低波
228 SZ.399664 中小高贝
229 SZ.399665 中创低波
230 SZ.399666 中创高贝
231 SZ.399667 创业板G
232 SZ.399668 创业板V
233 SZ.399669 深证农业
234 SZ.399670 深周期50
235 SZ.399671 深防御50
236 SZ.399672 深红利50
237 SZ.399673 创业板50
238 SZ.399674 深A医药
239 SZ.399675 深互联网
240 SZ.399676 深医药EW
241 SZ.399677 深互联EW
242 SZ.399678 深次新股
243 SZ.399679 深证200R
244 SZ.399680 深成能源
245 SZ.399681 深成材料
246 SZ.399682 深成工业
247 SZ.399683 深成可选
248 SZ.399684 深成消费
249 SZ.399685 深成医药
250 SZ.399686 深成金融
251 SZ.399687 深成信息
252 SZ.399688 深成电信
253 SZ.399689 深成公用
254 SZ.399690 中小专利
255 SZ.399691 创业专利
256 SZ.399692 创业低波
257 SZ.399693 安防产业
258 SZ.399694 创业高贝
259 SZ.399695 深证节能
260 SZ.399696 深证创投
261 SZ.399697 中关村60
262 SZ.399698 优势成长
263 SZ.399699 金融科技
264 SZ.399701 深证F60
265 SZ.399702 深证F120
266 SZ.399703 深证F200
267 SZ.399704 深证上游
268 SZ.399705 深证中游
269 SZ.399706 深证下游
270 SZ.399707 CSSW证券
271 SZ.399802 500深市
272 SZ.399803 工业4.0
273 SZ.399804 中证体育
274 SZ.399805 互联金融
275 SZ.399806 环境治理
276 SZ.399807 高铁产业
277 SZ.399808 中证新能
278 SZ.399809 保险主题
279 SZ.399810 CSSW传媒
280 SZ.399811 CSSW电子
281 SZ.399812 养老产业
282 SZ.399813 中证国安
283 SZ.399814 大农业
284 SZ.399817 生态100
285 SZ.399901 小康指数
286 SZ.399902 中证流通
287 SZ.399903 中证100
288 SZ.399904 中证200
289 SZ.399905 中证500
290 SZ.399906 中证800
291 SZ.399907 中证700
292 SZ.399908 300能源
293 SZ.399909 300材料
294 SZ.399910 300工业
295 SZ.399911 300可选
296 SZ.399912 300消费
297 SZ.399913 300医药
298 SZ.399914 300金融
299 SZ.399915 300信息
300 SZ.399916 300电信
301 SZ.399917 300公用
302 SZ.399918 300成长
303 SZ.399919 300价值
304 SZ.399920 300R成长
305 SZ.399921 300R价值
306 SZ.399922 中证红利
307 SZ.399923 公司债指
308 SZ.399924 分离债指
309 SZ.399925 基本面50
310 SZ.399926 中证央企
311 SZ.399927 央企100
312 SZ.399928 中证能源
313 SZ.399929 中证材料
314 SZ.399930 中证工业
315 SZ.399931 中证可选
316 SZ.399932 中证消费
317 SZ.399933 中证医药
318 SZ.399934 中证金融
319 SZ.399935 中证信息
320 SZ.399936 中证电信
321 SZ.399937 中证公用
322 SZ.399938 中证民企
323 SZ.399939 民企200
324 SZ.399940 财富大盘
325 SZ.399941 新能源
326 SZ.399942 内地消费
327 SZ.399943 内地基建
328 SZ.399944 内地资源
329 SZ.399945 内地运输
330 SZ.399946 内地金融
331 SZ.399947 内地银行
332 SZ.399948 内地地产
333 SZ.399949 内地农业
334 SZ.399950 300基建
335 SZ.399951 300银行
336 SZ.399952 300地产
337 SZ.399953 中证地企
338 SZ.399954 地企100
339 SZ.399955 中证国企
340 SZ.399956 国企200
341 SZ.399957 300运输
342 SZ.399958 创业成长
343 SZ.399959 军工指数
344 SZ.399960 中证龙头
345 SZ.399961 中证上游
346 SZ.399962 中证中游
347 SZ.399963 中证下游
348 SZ.399964 中证新兴
349 SZ.399965 800地产
350 SZ.399966 800非银
351 SZ.399967 中证军工
352 SZ.399968 300周期
353 SZ.399969 300非周
354 SZ.399970 移动互联
355 SZ.399971 中证传媒
356 SZ.399972 300深市
357 SZ.399973 中证国防
358 SZ.399974 国企改革
359 SZ.399975 证券公司
360 SZ.399976 CS新能车
361 SZ.399977 内地低碳
362 SZ.399978 医药100
363 SZ.399979 大宗商品
364 SZ.399980 中证超大
365 SZ.399981 300分层
366 SZ.399982 500等权
367 SZ.399983 地产等权
368 SZ.399984 300等权
369 SZ.399985 中证全指
370 SZ.399986 中证银行
371 SZ.399987 中证酒
372 SZ.399989 中证医疗
373 SZ.399990 煤炭等权
374 SZ.399991 一带一路
375 SZ.399992 CSWD并购
376 SZ.399993 CSWD生科
377 SZ.399994 信息安全
378 SZ.399995 基建工程
379 SZ.399996 智能家居
380 SZ.399997 中证白酒
381 SZ.399998 中证煤炭
'''

''' HK INDEX
 code name
0 HK.100000 黄金期货
1 HK.100100 石油期货
2 HK.100200 白银期货
3 HK.100300 铂金期货
4 HK.100400 MTW
5 HK.100500 TCB
6 HK.100600 TCE
7 HK.100700 TCF
8 HK.100701 人民币汇率
9 HK.100702 欧美汇率
10 HK.100703 澳美汇率
11 HK.800000 恒生指数
12 HK.800100 国企指数
13 HK.800121 标普香港大型股指数
14 HK.800122 沪深300指数
15 HK.800123 中证香港100指数
16 HK.800124 中证两岸三地500指数
17 HK.800125 恒指波幅指数
18 HK.800126 中证内地消费指数
19 HK.800127 中证香港内地民营企业指数
20 HK.800129 中证香港上市可交易内地地产指数
21 HK.800130 中证香港上市可交易内地消费指数
22 HK.800131 中证海外内地股港元指数
23 HK.800132 中证香港红利港币指数
24 HK.800133 中证锐联香港基本面50港币指数
25 HK.800134 中证香港中盘精选港币指数
26 HK.800135 中证香港内地股港元指数
27 HK.800136 中华交易服务中国120指数
28 HK.800137 中华交易服务中国A80指数
29 HK.800138 中华交易服务中国香港内地指数
30 HK.800139 中证香港内地国有企业指数
31 HK.800140 标普香港创业板指数
32 HK.800141 上证180指数
33 HK.800142 上证180公司治理指数
34 HK.800143 上证380指数
35 HK.800144 上证50指数
36 HK.800145 上证大宗商品股票指数
37 HK.800146 上证综合指数
38 HK.800147 上证红利指数
39 HK.800148 上证龙头企业指数
40 HK.800149 上证中盘指数
41 HK.800150 上证超级大盘指数
42 HK.800151 红筹指数
43 HK.800152 恒生金融分类指数
44 HK.800153 恒生公用分类指数
45 HK.800154 恒生地产分类指数
46 HK.800155 恒生工商分类指数
47 HK.800200 道琼斯指数
'''

''' US INDEX
0 US..IXIC 纳斯达克综合指数
1 US..DJI 道琼斯指数
2 US..DJT 道琼斯交通运输平均指数
3 US..DJU 道琼斯公用事业平均指数
4 US..DJUS 道琼斯美国指数
5 US..DJUSAE 道琼斯美国航空航天与国防指数
6 US..DJUSAF "Dow Jones US Delivery Services Index
"
7 US..DJUSAG "Dow Jones US Asset Managers Index
"
8 US..DJUSAI 道琼斯美国电子设备指数
9 US..DJUSAL 道琼斯美铝指数
10 US..DJUSAM 道琼斯美国医疗器械指数
11 US..DJUSAP 道琼斯美国汽车零部件指数
12 US..DJUSAR 道琼斯美国航空指数
13 US..DJUSAS 道琼斯美国航空航天指数
14 US..DJUSAT 道琼斯美国汽车零部件指数
15 US..DJUSAU 道琼斯美国汽车指数
16 US..DJUSAV 道琼斯美国媒体代理指数
17 US..DJUSBC 道琼斯美国广播娱乐指数
18 US..DJUSBD 道琼斯美国建筑材料及灯具指数
19 US..DJUSBE "Dow Jones US Business Training & Employment Agencies Index
"
20 US..DJUSBK 道琼斯美国银行指数
21 US..DJUSBM 道琼斯美国基础材料指数
22 US..DJUSBS 道琼斯美国基础资源指数
23 US..DJUSBT 道琼斯美国生物技术指数
24 US..DJUSBV 道琼斯美国饮料指数
25 US..DJUSCA 道琼斯美国赌博指数
26 US..DJUSCC 道琼斯美国商品化学品指数
27 US..DJUSCF 道琼斯美国服装配饰指数
28 US..DJUSCG 道琼斯美国旅游休闲指数
29 US..DJUSCH 道琼斯美国化学品指数
30 US..DJUSCL 道琼斯美国煤炭指数
31 US..DJUSCM 道琼斯美国个人产品指数
32 US..DJUSCN 道琼斯美国建筑材料指数
33 US..DJUSCP 道琼斯美国集装箱和包装指数
34 US..DJUSCR 道琼斯美国计算机硬件指数
35 US..DJUSCS 道琼斯美国专业消费者服务指数
36 US..DJUSCT 道琼斯美国电讯设备指数
37 US..DJUSCX 道琼斯美国特种化学品指数
38 US..DJUSCY 道琼斯美国消费者服务指数
39 US..DJUSDB 道琼斯美国酿酒商指数
40 US..DJUSDN 道琼斯美国国防指数
41 US..DJUSDR 道琼斯美国食品和药物零售商指数
42 US..DJUSDS 道琼斯美国工业供应商指数
43 US..DJUSDT "Dow Jones US Diversified REITs
"
44 US..DJUSDV 道琼斯美国计算机服务指数
45 US..DJUSEC 道琼斯美国电气元件及设备指数
46 US..DJUSEE 道琼斯美国电子电气设备指数
47 US..DJUSEH 道琼斯美国房地产控股与发展指数
48 US..DJUSEN 道琼斯美国石油和天然气指数
49 US..DJUSES 道琼斯美国房地产服务指数
50 US..DJUSEU 道琼斯美国电力指数
51 US..DJUSFA "Dow Jones US Financial Administration Index
"
52 US..DJUSFB 道琼斯美国食品和饮料指数
53 US..DJUSFC 道琼斯美国固定电话电讯指数
54 US..DJUSFD 道琼斯美国食品零售商和批发商指数
55 US..DJUSFE 道琼斯美国工业机械指数
56 US..DJUSFH 道琼斯美国家具指数
57 US..DJUSFI 道琼斯美国金融服务指数
58 US..DJUSFN 道琼斯美国金融指数
59 US..DJUSFO 道琼斯美国食品生产商指数
60 US..DJUSFP 道琼斯美国食品指数
61 US..DJUSFR 道琼斯美国林业与纸业指数
62 US..DJUSFT 道琼斯美国鞋业指数
63 US..DJUSFV 道琼斯美国金融服务综合指数
64 US..DJUSGF 道琼斯美国通用财务指数
65 US..DJUSGI 道琼斯美国通用工业指数
66 US..DJUSGL "Dow Jones US Large-Cap Growth Index
"
67 US..DJUSGM "Dow Jones US Mid-Cap Growth Index
"
68 US..DJUSGR 道琼斯美国增长指数
69 US..DJUSGS "Dow Jones US Small-Cap Growth Index
"
70 US..DJUSGT 道琼斯美国综合零售商指数
71 US..DJUSGU 道琼斯美国天然气分布指数
72 US..DJUSHB 道琼斯美国家居建筑指数
73 US..DJUSHC 道琼斯美国医疗保健指数
74 US..DJUSHD 道琼斯美国耐用家用产品指数
75 US..DJUSHG 道琼斯美国家庭用品指数
76 US..DJUSHI 道琼斯美国家居零售商指数
77 US..DJUSHL "Dow Jones US Hotel & Lodging REITs Index
"
78 US..DJUSHN 道琼斯美国无保障住户产品指数
79 US..DJUSHP 道琼斯美国医疗保健提供者指数
80 US..DJUSHR 道琼斯美国商用车辆和卡车指数
81 US..DJUSHV 道琼斯美国重型建筑指数
82 US..DJUSIB 道琼斯美国保险经纪人指数
83 US..DJUSID 道琼斯美国多元化工业指数
84 US..DJUSIF 道琼斯美国全线保险指数
85 US..DJUSIG 道琼斯美国工业产品与服务
86 US..DJUSIL 道琼斯美国人寿保险指数
87 US..DJUSIM 道琼斯美国工业金属指数
88 US..DJUSIN 道琼斯美国工业指数
89 US..DJUSIO 道琼斯美国工业和房地产信托指数
90 US..DJUSIP 道琼斯美国财险保险指数
91 US..DJUSIQ 道琼斯美国工业工程指数
92 US..DJUSIR 道琼斯美国保险指数
93 US..DJUSIS 道琼斯美国支持服务指数
94 US..DJUSIT 道琼斯美国工业交通指数
95 US..DJUSIU 道琼斯美国再保险指数
96 US..DJUSIV 道琼斯美国商业支持服务指数
97 US..DJUSIX 道琼斯美国非人寿保险指数
98 US..DJUSL 道琼斯美国大盘指数
99 US..DJUSLE 道琼斯美国休闲用品指数
100 US..DJUSLG 道琼斯美国酒店指数
101 US..DJUSLTR "Dow Jones US Large-Cap TR Index (EOD)
"
102 US..DJUSLW "Dow Jones US Low Cap Index
"
103 US..DJUSM "Dow Jones US Mid Cap Index
"
104 US..DJUSMC 道琼斯美国医疗保健设备及服务指数
105 US..DJUSME 道琼斯美国媒体指数
106 US..DJUSMF 道琼斯美国抵押贷款融资指数
107 US..DJUSMG 道琼斯美国矿业指数
108 US..DJUSMR 道琼斯美国抵押房地产信托指数
109 US..DJUSMS 道琼斯美国医疗用品指数
110 US..DJUSMT 道琼斯美国海运交通指数
111 US..DJUSMU 道琼斯美国多功能指数
112 US..DJUSNC 道琼斯美国消费品指数
113 US..DJUSNF 道琼斯美国有色金属指数
114 US..DJUSNG 道琼斯美国个人及家庭用品指数
115 US..DJUSNS 道琼斯美国互联网指数
116 US..DJUSOE 道琼斯美国电子办公设备指数
117 US..DJUSOG 道琼斯美国石油和天然气生产商指数
118 US..DJUSOI 道琼斯美国石油设备与服务指数
119 US..DJUSOL 道琼斯美国综合油气指数
120 US..DJUSOQ 道琼斯美国石油设备，服务与分销指数
121 US..DJUSOS 道琼斯美国勘探与生产指数
122 US..DJUSPB 道琼斯美国出版指数
123 US..DJUSPC 道琼斯美国废物处理服务指数
124 US..DJUSPG 道琼斯美国个人商品指数
125 US..DJUSPL 道琼斯美国管道指数
126 US..DJUSPM 道琼斯美国黄金矿业指数
127 US..DJUSPN 道琼斯美国制药与生物技术指数
128 US..DJUSPP 道琼斯美国纸业指数
129 US..DJUSPR 道琼斯美国医药指数
130 US..DJUSPT 道琼斯美国铂金与贵金属指数
131 US..DJUSRA 道琼斯美国服装零售商指数
132 US..DJUSRB "Dow Jones US Broadline Retailers Index
"
133 US..DJUSRD 道琼斯美国药物零售商指数
134 US..DJUSRE "Dow Jones US Real Estate Index
"
135 US..DJUSRH 道琼斯美国房地产投资与服务指数
136 US..DJUSRI 道琼斯美国房地产投资信托指数
137 US..DJUSRL "Dow Jones US Retail REITs Index
"
138 US..DJUSRN "Dow Jones US Residential REITs Index
"
139 US..DJUSRP 道琼斯美国娱乐产品指数
140 US..DJUSRQ 道琼斯美国娱乐服务指数
141 US..DJUSRR 道琼斯美国铁路指数
142 US..DJUSRS 道琼斯美国专业零售商指数
143 US..DJUSRT 道琼斯美国零售指数
144 US..DJUSRU 道琼斯美国餐馆和酒吧指数
145 US..DJUSS "Dow Jones US Small Cap Index
"
146 US..DJUSSB 道琼斯美国投资服务公司
147 US..DJUSSC 道琼斯美国半导体指数
148 US..DJUSSD 道琼斯美国软饮料指数
149 US..DJUSSF 道琼斯美国消费者金融指数
150 US..DJUSSP 道琼斯美国专业金融指数
151 US..DJUSSR "Dow Jones US Specialty REITs Index
"
152 US..DJUSST 道琼斯美国钢铁指数
153 US..DJUSSV 道琼斯美国软件与计算机服务指数
154 US..DJUSSW 道琼斯美国软件指数
155 US..DJUSTB 道琼斯美国烟草指数
156 US..DJUSTC 道琼斯美国高科技指数
157 US..DJUSTK 道琼斯美国卡车指数
158 US..DJUSTL 道琼斯美国电讯指数
159 US..DJUSTP "Dow Jones US Top Cap Index
"
160 US..DJUSTQ 道琼斯美国技术五金设备指数
161 US..DJUSTR 道琼斯美轮胎指数
162 US..DJUSTS 道琼斯美国运输服务指数
163 US..DJUSTT 道琼斯美国旅游与旅游指数
164 US..DJUSTY 道琼斯美国玩具指数
165 US..DJUSUO "Dow Jones US Gas"
166 US..DJUSUT 道琼斯美国公用事业指数
167 US..DJUSVA 道琼斯美国价值指数
168 US..DJUSVE 道琼斯美国常规电力指数
169 US..DJUSVL "Dow Jones US Large-Cap Value Index
"
170 US..DJUSVM "Dow Jones US Mid-Cap Value Index
"
171 US..DJUSVN 道琼斯美国酿酒商与葡萄酒指数
172 US..DJUSVS "Dow Jones US Small-Cap Value Index
"
173 US..DJUSWC 道琼斯美国移动通信指数
174 US..DJUSWU 道琼斯美国水资源指数
175 US..INX 标普500指数
176 US..NDX 纳斯达克100
177 US..SP100 标普100指数
178 US..SPCMI 标普完整指数
179 US..W5000FLT "Wilshire 5000 Total Market Index
"
180 US..W5KLC "Wilshire US Large Cap
"
181 US..W5KMC "Wilshire US Mid Cap
"
182 US..W5KMICRO "Wilshire US Micro Cap
"
183 US..W5KSC "Wilshire US Small Cap
"
184 US..AEX 荷兰AEX指数
185 US..AS51 澳大利亚指数
186 US..ASE 希腊指数
187 US..ATX 奥地利指数
188 US..BEL20 比利时指数
189 US..CAC 法国指数
190 US..CASE 埃及指数
191 US..CSEALL 斯里兰卡指数
192 US..DAX 德国DAX指数
193 US..FBMKLC 马来西亚指数
194 US..FSSTI 新加坡指数
195 US..FTSEMIB 意大利指数
196 US..HEX 芬兰指数
197 US..IBEX 西班牙指数
198 US..IBOV 巴西指数
199 US..ICEXI 冰岛指数
200 US..INDEXCF 俄罗斯指数
201 US..ISEQ 爱尔兰指数
202 US..JCI 印尼综合指数
203 US..KFX 丹麦指数
204 US..kospi 韩国指数
205 US..KSE100 巴基斯坦指数
206 US..LUXXX 卢森堡指数
207 US..MEXBOL 墨西哥指数
208 US..NKY 日经225指数
209 US..NZSE50FG 新西兰指数
210 US..OBX 挪威指数
211 US..OMX 瑞典OMX指数
212 US..PCOMP 菲律宾指数
213 US..PX 捷克指数
214 US..SENSEX 印度孟买指数
215 US..SET 泰国指数
216 US..SMI 瑞士市场指数
217 US..SPTSX 加拿大指数
218 US..TWSE 台湾加权指数
219 US..UKX 英国指数
220 US..VNINDEX 越南指数
221 US..WIG 波兰指数
'''


# def fetch2DB():

def enum_all_stocks(ip, port):
    quote_ctx = OpenQuoteContext(ip, port)

    ret, dfm_stocklist_hk = quote_ctx.get_stock_basicinfo(market='HK', stock_type='STOCK')
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch stock list of HK, error message: %s' %dfm_stocklist_hk

    ret, dfm_stocklist_us = quote_ctx.get_stock_basicinfo(market='US', stock_type='STOCK')
    if ret == RET_ERROR:
        assert 0==1, 'Error during fetch stock list of US, error message: %s' %dfm_stocklist_us
    quote_ctx.close()
    return pd.concat([dfm_stocklist_hk,dfm_stocklist_us])

def enum_all_index(ip, port):
    quote_ctx = OpenQuoteContext(ip, port)

    ret, data_frame = quote_ctx.get_stock_basicinfo(market='SH', stock_type='STOCK')
    data_frame.to_csv("index_sh.txt", index=True, sep=' ')
    print('market SH index data saved!')

    ret, data_frame = quote_ctx.get_stock_basicinfo(market='SZ', stock_type='IDX')
    data_frame.to_csv("index_sz.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market SZ index data saved!')

    ret, data_frame = quote_ctx.get_stock_basicinfo(market='HK', stock_type='IDX')
    data_frame.to_csv("index_hk.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market HK index data saved!')

    ret, data_frame = quote_ctx.get_stock_basicinfo(market='US', stock_type='IDX')
    data_frame.to_csv("index_us.txt", index=True, sep=' ', columns=['code', 'name'])
    print('market US index data saved!')

    quote_ctx.close()


def get_index_stocks(ip, port, strcode):
    quote_ctx = OpenQuoteContext(ip, port)
    ret, data_frame = quote_ctx.get_plate_stock(strcode)
    quote_ctx.close()
    return ret, data_frame

if __name__ == "__main__":
    # enum_all_index(api_ip, api_port)

    dfm_stocklist = enum_all_stocks(api_ip, api_port)
    dfm_stocklist.to_excel('stocklist.xls')

    # print('SH.000001 上证指数 \n')
    # print(get_index_stocks(api_ip, api_port, 'SH.000001'))
    #
    # print('SZ.399006 创业板指\n')
    # print(get_index_stocks(api_ip, api_port, 'SZ.399006'))
    #
    # print('HK.800000 恒生指数 \n')
    # print(get_index_stocks(api_ip, api_port, 'HK.800000'))
    #
    # print('US..DJI 道琼斯指数\n')
    # print(get_index_stocks(api_ip, api_port, 'US..DJI'))





