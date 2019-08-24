

select event_date, event_param.key, event_param.value.string_value, count(event_timestamp) as count
from `bunjang-68108.analytics_153554412.events_*`,
  UNNEST(event_params) as event_param
where event_name = 'select_content'
  and _TABLE_SUFFIX BETWEEN '20190101' and '20190820'
  and (event_param.key = 'item_id' and event_param.value.string_value ='연락하기') 
group by event_date, event_param.key, event_param.value.string_value
order by event_date
