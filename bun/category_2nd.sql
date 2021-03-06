select cast(date_trunc('month', gr_tr.updated_at) as date) as months,
gr_tr.type,
case 
    when gr_tr.category_1st = '410' then '뷰티_미용'
    when gr_tr.category_1st = '400' then '패션잡화'
    when gr_tr.category_1st = '600' then '디지털_가전'
    when gr_tr.category_1st = '310' then '여성의류'
    when gr_tr.category_1st = '320' then '남성의류'
    when gr_tr.category_1st = '240' then '구인구직'
    when gr_tr.category_1st = '750' then '차량_오토바이'
    when gr_tr.category_1st = '700' then '스포츠_레저'
    when gr_tr.category_1st = '500' then '유아동_출산'
    when gr_tr.category_1st = '220' then '지역_서비스'
    when gr_tr.category_1st = '910' then '스타굿즈'
    when gr_tr.category_1st = '900' then '도서_티켓_취미_애완'
    when gr_tr.category_1st = '800' then '생활_문구_가구_식품'
    when gr_tr.category_1st = '300' then '구_패션의류'
    when gr_tr.category_1st = '230' then '원룸_함께살아요'
    when gr_tr.category_1st = '210' then '재능'
    when gr_tr.category_1st = '999' then '기타'
    when gr_tr.category_1st = '100' then '커뮤니티'
    when gr_tr.category_1st = '200' then '번개나눔'
else 'no_category' end as category_1st,
case
    when gr_tr.category_2nd = '100' then '커뮤니티'
    when gr_tr.category_2nd = '100200' then '수다방'
    when gr_tr.category_2nd = '100400' then '칭찬해요(후기)'
    when gr_tr.category_2nd = '100600' then '주의해요(후기)'
    when gr_tr.category_2nd = '100800' then '광고방/상점공지'
    when gr_tr.category_2nd = '200' then '번개나눔'
    when gr_tr.category_2nd = '200100' then '물품 무료나눔'
    when gr_tr.category_2nd = '200200' then '먹거리 무료나눔'
    when gr_tr.category_2nd = '200300' then '덤(조건부나눔)'
    when gr_tr.category_2nd = '200400' then '당첨발표(당발)'
    when gr_tr.category_2nd = '200999' then '기타'
    when gr_tr.category_2nd = '210' then '재능'
    when gr_tr.category_2nd = '210211' then '스타일/뷰티'
    when gr_tr.category_2nd = '210212' then '디자인/영상/사진'
    when gr_tr.category_2nd = '210214' then '생활서비스/지식'
    when gr_tr.category_2nd = '210215' then '블로그/문서/번역'
    when gr_tr.category_2nd = '210216' then '거래 대행'
    when gr_tr.category_2nd = '210217' then '기타 재능'
    when gr_tr.category_2nd = '210218' then '재능인 찾아요'
    when gr_tr.category_2nd = '220' then '지역 서비스'
    when gr_tr.category_2nd = '220010' then '네일/미용'
    when gr_tr.category_2nd = '220020' then '호텔/펜션/숙박'
    when gr_tr.category_2nd = '220030' then '학원/수강'
    when gr_tr.category_2nd = '220050' then '헬스/요가'
    when gr_tr.category_2nd = '220060' then '병원/약국'
    when gr_tr.category_2nd = '220070' then '차량/수리'
    when gr_tr.category_2nd = '220080' then '이사/용달'
    when gr_tr.category_2nd = '220090' then '결혼/행사'
    when gr_tr.category_2nd = '220100' then '청소/세탁/철거'
    when gr_tr.category_2nd = '220110' then '금융/채무'
    when gr_tr.category_2nd = '220999' then '기타'
    when gr_tr.category_2nd = '230010' then '원룸/투룸'
    when gr_tr.category_2nd = '230020' then '오피스텔'
    when gr_tr.category_2nd = '230030' then '주택/빌라'
    when gr_tr.category_2nd = '230040' then '아파트'
    when gr_tr.category_2nd = '230050' then '점포/상가'
    when gr_tr.category_2nd = '230060' then '기타(부동산)'
    when gr_tr.category_2nd = '230070' then '룸/하우스메이트(남성)'
    when gr_tr.category_2nd = '230080' then '룸/하우스메이트(여성)'
    when gr_tr.category_2nd = '230999' then '기타(메이트)'
    when gr_tr.category_2nd = '240010' then '매장관리'
    when gr_tr.category_2nd = '240020' then '서빙/주방'
    when gr_tr.category_2nd = '240030' then '서비스/미디어'
    when gr_tr.category_2nd = '240040' then '사무/회계'
    when gr_tr.category_2nd = '240050' then '생산/기능직'
    when gr_tr.category_2nd = '240060' then 'IT/디자인'
    when gr_tr.category_2nd = '240070' then '상담영업'
    when gr_tr.category_2nd = '240080' then '강사/교육'
    when gr_tr.category_2nd = '240090' then '재택알바'
    when gr_tr.category_2nd = '240100' then '알바찾아요'
    when gr_tr.category_2nd = '300100' then '(구)여성의류'
    when gr_tr.category_2nd = '300200' then '(구)남성의류'
    when gr_tr.category_2nd = '310' then '여성의류'
    when gr_tr.category_2nd = '310010' then '긴팔 티셔츠'
    when gr_tr.category_2nd = '310020' then '반팔 티셔츠'
    when gr_tr.category_2nd = '310030' then '맨투맨/후드티'
    when gr_tr.category_2nd = '310040' then '블라우스'
    when gr_tr.category_2nd = '310050' then '셔츠/남방'
    when gr_tr.category_2nd = '310060' then '니트/스웨터'
    when gr_tr.category_2nd = '310070' then '가디건'
    when gr_tr.category_2nd = '310080' then '조끼/베스트'
    when gr_tr.category_2nd = '310090' then '야상/점퍼/패딩'
    when gr_tr.category_2nd = '310100' then '자켓'
    when gr_tr.category_2nd = '310110' then '코트'
    when gr_tr.category_2nd = '310120' then '원피스'
    when gr_tr.category_2nd = '310130' then '스커트/치마'
    when gr_tr.category_2nd = '310140' then '청바지/스키니(긴)'
    when gr_tr.category_2nd = '310150' then '면/캐주얼 바지(긴)'
    when gr_tr.category_2nd = '310160' then '반바지/핫팬츠'
    when gr_tr.category_2nd = '310170' then '레깅스'
    when gr_tr.category_2nd = '310180' then '비즈니스 정장'
    when gr_tr.category_2nd = '310190' then '트레이닝'
    when gr_tr.category_2nd = '310200' then '언더웨어/속옷'
    when gr_tr.category_2nd = '310210' then '빅사이즈'
    when gr_tr.category_2nd = '310220' then '테마/이벤트 의류'
    when gr_tr.category_2nd = '320' then '남성의류'
    when gr_tr.category_2nd = '320010' then '긴팔 티셔츠'
    when gr_tr.category_2nd = '320020' then '반팔 티셔츠'
    when gr_tr.category_2nd = '320030' then '맨투맨/후드티'
    when gr_tr.category_2nd = '320040' then '셔츠/남방'
    when gr_tr.category_2nd = '320050' then '니트/스웨터'
    when gr_tr.category_2nd = '320060' then '가디건 '
    when gr_tr.category_2nd = '320070' then '조끼/베스트'
    when gr_tr.category_2nd = '320080' then '점퍼/야상/패딩'
    when gr_tr.category_2nd = '320090' then '자켓'
    when gr_tr.category_2nd = '320100' then '코트'
    when gr_tr.category_2nd = '320110' then '청바지(긴)'
    when gr_tr.category_2nd = '320120' then '면/캐주얼 바지(긴)'
    when gr_tr.category_2nd = '320130' then '반바지/7~9부'
    when gr_tr.category_2nd = '320140' then '비즈니스 정장'
    when gr_tr.category_2nd = '320150' then '트레이닝'
    when gr_tr.category_2nd = '320160' then '언더웨어/속옷'
    when gr_tr.category_2nd = '320170' then '빅사이즈'
    when gr_tr.category_2nd = '320180' then '테마/이벤트 의류'
    when gr_tr.category_2nd = '400' then '패션잡화'
    when gr_tr.category_2nd = '400010' then '여성가방'
    when gr_tr.category_2nd = '400020' then '남성가방'
    when gr_tr.category_2nd = '400030' then '여행용가방/소품'
    when gr_tr.category_2nd = '400040' then '운동화/캐주얼화'
    when gr_tr.category_2nd = '400050' then '여성화'
    when gr_tr.category_2nd = '400051' then '남성화'
    when gr_tr.category_2nd = '400060' then '지갑 '
    when gr_tr.category_2nd = '400070' then '모자'
    when gr_tr.category_2nd = '400080' then '안경/선글라스'
    when gr_tr.category_2nd = '400081' then '주얼리/액세서리'
    when gr_tr.category_2nd = '400082' then '시계'
    when gr_tr.category_2nd = '400083' then '벨트/장갑/스타킹/기타'
    when gr_tr.category_2nd = '400100' then '(구)패션잡화'
    when gr_tr.category_2nd = '400200' then '(구)신발'
    when gr_tr.category_2nd = '400300' then '(구)가방/지갑'
    when gr_tr.category_2nd = '400400' then '(구)쥬얼리/시계'
    when gr_tr.category_2nd = '400500' then '(구)뷰티'
    when gr_tr.category_2nd = '410' then '뷰티/미용'
    when gr_tr.category_2nd = '410100' then '스킨케어'
    when gr_tr.category_2nd = '410200' then '썬케어'
    when gr_tr.category_2nd = '410300' then '베이스메이크업'
    when gr_tr.category_2nd = '410400' then '색조메이크업'
    when gr_tr.category_2nd = '410500' then '향수/아로마'
    when gr_tr.category_2nd = '410600' then '헤어/바디'
    when gr_tr.category_2nd = '410700' then '네일아트/케어'
    when gr_tr.category_2nd = '410800' then '이미용품/미용 기기'
    when gr_tr.category_2nd = '410900' then '다이어트/이색 뷰티'
    when gr_tr.category_2nd = '410950' then '남성 화장품'
    when gr_tr.category_2nd = '500' then '유아동/출산'
    when gr_tr.category_2nd = '500100' then '유아/출산'
    when gr_tr.category_2nd = '500110' then '베이비의류(0-2세)'
    when gr_tr.category_2nd = '500111' then '여아의류(3-6세)'
    when gr_tr.category_2nd = '500113' then '남아의류(3-6세)'
    when gr_tr.category_2nd = '500114' then '여주니어의류(7세~)'
    when gr_tr.category_2nd = '500115' then '남주니어의류(7세~)'
    when gr_tr.category_2nd = '500116' then '유아동신발/잡화'
    when gr_tr.category_2nd = '500117' then '유아동용품'
    when gr_tr.category_2nd = '500118' then '출산/임부용품'
    when gr_tr.category_2nd = '500119' then '교육/완구/인형'
    when gr_tr.category_2nd = '500120' then '기저귀/수유/이유식'
    when gr_tr.category_2nd = '600' then '디지털/가전'
    when gr_tr.category_2nd = '600100' then '노트북/넷북'
    when gr_tr.category_2nd = '600200' then 'PC/모니터/주변기기'
    when gr_tr.category_2nd = '600300' then '카메라/DSLR'
    when gr_tr.category_2nd = '600400' then '가전제품'
    when gr_tr.category_2nd = '600500' then '음반/영상/관련기기'
    when gr_tr.category_2nd = '600600' then '게임/타이틀'
    when gr_tr.category_2nd = '600700' then '모바일'
    when gr_tr.category_2nd = '700' then '스포츠/레저'
    when gr_tr.category_2nd = '700100' then '축구/야구/농구'
    when gr_tr.category_2nd = '700200' then '낚시/캠핑 용품'
    when gr_tr.category_2nd = '700350' then '자전거/MTB'
    when gr_tr.category_2nd = '700400' then '전동킥보드/전동휠'
    when gr_tr.category_2nd = '700500' then '인라인/스케이트보드'
    when gr_tr.category_2nd = '700600' then '헬스/요가/골프'
    when gr_tr.category_2nd = '700700' then '등산'
    when gr_tr.category_2nd = '700800' then '수영'
    when gr_tr.category_2nd = '700900' then '스키/스노우보드'
    when gr_tr.category_2nd = '700950' then '기타구기 스포츠'
    when gr_tr.category_2nd = '750' then '차량/오토바이'
    when gr_tr.category_2nd = '750100' then '수입차(개인)'
    when gr_tr.category_2nd = '750200' then '국산차(개인)'
    when gr_tr.category_2nd = '750210' then '국산차(딜러)'
    when gr_tr.category_2nd = '750300' then '네비게이션/블랙박스'
    when gr_tr.category_2nd = '750400' then '카오디오/영상'
    when gr_tr.category_2nd = '750500' then '타이어/휠'
    when gr_tr.category_2nd = '750600' then '차량/튜닝 용품'
    when gr_tr.category_2nd = '750700' then '차량 부품'
    when gr_tr.category_2nd = '750800' then '오토바이/스쿠터'
    when gr_tr.category_2nd = '750900' then '산업용품'
    when gr_tr.category_2nd = '800' then '생활/문구/가구/식품'
    when gr_tr.category_2nd = '800100' then '생활용품'
    when gr_tr.category_2nd = '800200' then '가구'
    when gr_tr.category_2nd = '800300' then '식품'
    when gr_tr.category_2nd = '800400' then '주방용품'
    when gr_tr.category_2nd = '900' then '도서/티켓/취미/애완'
    when gr_tr.category_2nd = '900100' then '도서/책'
    when gr_tr.category_2nd = '900200' then '(구)티켓/할인권'
    when gr_tr.category_2nd = '900210' then '티켓/항공권'
    when gr_tr.category_2nd = '900220' then '기프티콘/쿠폰'
    when gr_tr.category_2nd = '900230' then '상품권'
    when gr_tr.category_2nd = '900300' then '예술/취미/애완'
    when gr_tr.category_2nd = '900310' then '예술/악기/수공예품'
    when gr_tr.category_2nd = '900320' then '취미/키덜트'
    when gr_tr.category_2nd = '900330' then '애완(반려)'
    when gr_tr.category_2nd = '900400' then '희귀/수집품'
    when gr_tr.category_2nd = '910' then '스타굿즈'
    when gr_tr.category_2nd = '910100' then '보이그룹'
    when gr_tr.category_2nd = '910200' then '걸그룹'
    when gr_tr.category_2nd = '910400' then '솔로(여)'
    when gr_tr.category_2nd = '910500' then '솔로(남)'
    when gr_tr.category_2nd = '910600' then '배우(남)'
    when gr_tr.category_2nd = '910700' then '배우(여)'
    when gr_tr.category_2nd = '910800' then '기타(방송인)'
    when gr_tr.category_2nd = '999' then '기타'
else 'no_category' end as category_2nd,
sum(gr_tr.total_price) as total_price,
count(*) as total_count
from (select
tr.updated_at, tr.type, left(pi.category_id, 3) as category_1st, left(pi.category_id, 6) as category_2nd, tr.total_price
from product_info_for_stats pi
join
(select bp_log.updated as updated_at, bp.seller_pid as pid, bp.seller_pid_price as total_price, 'bunp' as type
from bunjang_promise bp
join 
(select bunp_id, updated
from bunp
where log_type = 'complite'
and updated >= current_date - interval '6 month') bp_log
on bp_log.bunp_id = bp.id
where bp.seller_pid_price < 3000000


union


select od.updated_at, oi.pid, od.total_price, 'bunpay' as type
from order_item oi
join
(select id, update_date as updated_at, total_price
from order_mast
where order_status_cd = 'purchase_confirm'
and update_date >= current_date - interval '6 month'
and (total_price - setl_fee_price) < 3000000) od
on od.id = oi.order_mast_id


UNION


select
updated_at, pid, product_price + transfer_fee + insurance_fee as total_price, 'transfer' as type
from wire_transfer
where status = 'transfer_completed'
and product_price < 3000000
and updated_at >= current_date - interval '6 month') tr
on pi.pid = tr.pid) gr_tr
group by months, gr_tr.type, gr_tr.category_1st, gr_tr.category_2nd