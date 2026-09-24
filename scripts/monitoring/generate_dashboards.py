"""Generate provisioned Grafana dashboards with bounded metric labels and explicit gaps."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / 'monitoring/grafana/dashboards'
PROM = {'type': 'prometheus', 'uid': 'platform-prometheus'}
LOKI = {'type': 'loki', 'uid': 'platform-loki'}
PAGES = [('overview','통합 관제'),('services','서비스'),('providers','외부 업체'),('customers','고객'),('errors','오류코드'),('infra','인프라'),('trace','메시지 추적')]
STAGE = 'delivery_stage_duration_seconds'
RATE = '$__rate_interval'
RANGE = '$__range'

def r(metric, labels=''):
    return f'rate({metric}{{{labels}}}[{RATE}])'

def total(metric, labels=''):
    return f'sum({r(metric,labels)})'

def quant(metric, labels='', q=.95, by=''):
    return f'histogram_quantile({q}, sum by (le{","+by if by else ""}) (rate({metric}_bucket{{{labels}}}[{RATE}])))'

class Dashboard:
    def __init__(self,uid,title):
        self.uid=uid; self.title=title; self.panels=[]; self.next_id=1
    def panel(self,title,expr,x,y,w=12,h=8,kind='timeseries',unit='short',description='',legend='{{stage}}',links=None,source=PROM):
        p={'id':self.next_id,'title':title,'type':kind,'gridPos':{'x':x,'y':y,'w':w,'h':h},'description':description,
           'datasource':source,'fieldConfig':{'defaults':{'unit':unit,'noValue':'데이터 없음','color':{'mode':'palette-classic'},'thresholds':{'mode':'absolute','steps':[{'color':'green','value':None},{'color':'orange','value':80},{'color':'red','value':100}]}},'overrides':[]},
           'targets':[{'refId':'A','expr':expr,'legendFormat':legend,'range':kind in ('timeseries','state-timeline'),'instant':kind in ('stat','table'),'format':'table' if kind=='table' else 'time_series'}]}
        if kind == 'stat':
            p['fieldConfig']['defaults']['mappings']=[{'type':'special','options':{'match':value,'result':{'text':'관측 없음','color':'gray'}}} for value in ('null','nan')]
            steps=None
            if 'Lag' in title or title=='③ 큐 처리 대기':steps=[(None,'green'),(100,'orange'),(500,'red')]
            if title=='활성 경보':steps=[(None,'green'),(1,'red')]
            if title=='접수 저장 지연 p95':steps=[(None,'green'),(1,'orange'),(3,'red')]
            if '성공률' in title:steps=[(None,'red'),(95,'orange'),(99,'green')]
            if '정상 여부' in title:steps=[(None,'red'),(1,'green')]
            if steps:
                p['fieldConfig']['defaults']['color']={'mode':'thresholds'}
                p['fieldConfig']['defaults']['thresholds']={'mode':'absolute','steps':[{'value':value,'color':color} for value,color in steps]}
        if links:p['links']=links
        if kind=='stat':p['options']={'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'orientation':'auto','textMode':'auto','colorMode':'value','graphMode':'area','justifyMode':'auto','text':{'valueSize':32,'titleSize':14}}
        if kind=='timeseries':p['options']={'legend':{'displayMode':'table','placement':'bottom','calcs':['lastNotNull']},'tooltip':{'mode':'multi','sort':'desc'}}
        if kind=='table':
            p['options']={'showHeader':True,'cellHeight':'sm'}
            p['transformations']=[{'id':'organize','options':{
                'excludeByName':{'Time':True,'__name__':True,'instance':True,'zone':True,'service':True},
                'renameByName':{'Value':'값','job':'대상','application':'앱','client_id':'Consumer','stage':'구간','status':'HTTP 상태','alertname':'경보','alertstate':'상태','severity':'심각도'}}}]
        self.next_id+=1;self.panels.append(p);return p
    def text(self,title,content,x,y,w=24,h=3):
        p={'id':self.next_id,'title':title,'type':'text','gridPos':{'x':x,'y':y,'w':w,'h':h},'options':{'mode':'markdown','content':content}}
        self.next_id+=1;self.panels.append(p);return p
    def logs(self,title,expr,x,y,w=24,h=10):
        p=self.panel(title,expr,x,y,w,h,kind='logs',source=LOKI)
        expr += ' | line_format "{{.stage}} · {{.outcome}} | deliveryId={{.deliveryId}} | tenant={{.tenantId}} | attempt={{.attemptId}} | {{.provider}} {{.code}}"'
        p['targets']=[{'refId':'A','expr':expr,'queryType':'range','maxLines':200}]
        p['options']={'showTime':True,'showLabels':False,'showCommonLabels':False,'wrapLogMessage':True,'prettifyLogMessage':True,'enableLogDetails':True,'sortOrder':'Descending','dedupStrategy':'none'}
        return p
    def save(self):
        board={'uid':'delivery-'+self.uid,'title':'발송 관제 · '+self.title,'schemaVersion':39,'version':1,'editable':False,'tags':['delivery-platform'],
               'timezone':'browser','refresh':'10s','time':{'from':'now-15m','to':'now'},'panels':self.panels,
               'links':[{'title':name,'url':'/d/delivery-'+uid,'type':'link','keepTime':True,'includeVars':True} for uid,name in PAGES],
               'templating':{'list':[]},'annotations':{'list':[{'name':'Annotations & Alerts','type':'dashboard','datasource':{'type':'grafana','uid':'-- Grafana --'},'enable':True,'hide':True,'iconColor':'rgba(255, 96, 96, 1)'}]}}
        if self.uid=='trace':board['templating']['list']=[{'name':'delivery','label':'메시지 ID (빈 값은 전체)','type':'textbox','query':'','current':{'text':'','value':''}}]
        if self.uid=='customers':board['templating']['list']=[{'name':'tenant','label':'고객 ID','type':'textbox','query':'1','current':{'text':'1','value':'1'}}]
        (OUT/(self.uid+'.json')).write_text(json.dumps(board,ensure_ascii=False,indent=2)+'\n')

def jump(uid):return [{'title':'상세 보기','url':'/d/delivery-'+uid,'targetBlank':False}]
def log_metric(d,title,expr,x,y,w=12,h=8):
    p=d.panel(title,expr,x,y,w,h,kind='table',source=LOKI)
    p['targets']=[{'refId':'A','expr':expr,'queryType':'instant'}]
    p['transformations']=[{'id':'labelsToFields','options':{'mode':'columns'}},
        {'id':'organize','options':{'excludeByName':{'Time':True},'renameByName':{'Value #A':'관측 횟수','tenantId':'고객 ID','provider':'업체','stage':'구간','code':'코드','outcome':'분류'}}}]
    return p

api=total('http_server_requests_seconds_count','application="delivery-api",uri="/api/v1/deliveries"')
accepted=total('delivery_outcomes_total','application="dispatch-worker",outcome="dispatch_accepted"')
http_success='100 * '+total(STAGE+'_count','application="dispatch-worker",stage="dispatch_http",result="success"')+' / '+total(STAGE+'_count','application="dispatch-worker",stage="dispatch_http"')
latency=quant('delivery_acceptance_latency_seconds','application="dispatch-worker"')
active_alerts='count(ALERTS{alertstate="firing"}) or vector(0)'
base_logs='{service=~"api|ingress|dispatch",zone="local"}'

d=Dashboard('overview','통합 관제')
d.text('발송 플랫폼 통합 관제','**LOCAL · HTTP 1차 접수 경로**　10초 새로고침 · 상태와 지표는 관측 시점 기준\n\n업체 접수는 최종 전달 완료와 다릅니다. [메시지 ID로 추적](/d/delivery-trace) · [서비스 상세](/d/delivery-services) · [인프라 상세](/d/delivery-infra)',0,0,24,3)
for i,(title,expr,unit,desc) in enumerate([
 ('인입 TPS',api,'reqps','인증 API의 전체 제출 요청. 재요청·거절도 포함'),('HTTP 업체 접수 성공률',http_success,'percent','실제 업체 호출 구간의 성공 비율. 최종 전달 성공률이 아님. 호출이 없으면 데이터 없음'),
 ('접수 저장 지연 p95',latency,'s','최초 인입부터 신규 업체 접수 결과 DB 저장 직후'),('Kafka 최대 파티션 Lag','max(platform_kafka_committed_lag)','short','Worker group의 committed offset 기준'),
 ('신규 접수 저장 TPS',accepted,'reqps','중복 skip 제외, 새로 저장한 업체 접수'),('활성 경보',active_alerts,'short','로컬 관찰용 규칙. 외부 통보 채널 미연결')]):
 d.panel(title,expr,i*4,3,4,4,'stat',unit,desc,links=jump('infra' if i in (3,5) else 'services'))
d.text('메시지 E2E 단계 · 상세 화면으로 이동','현재 구현된 1~4단계와 후속 업무를 구분합니다. 회색 단계는 **미구현**이며 정상 0건을 뜻하지 않습니다.',0,7,24,2)
for i,(name,expr) in enumerate([('① API 인입',api),('② 원본 저장',total('delivery_outcomes_total','outcome="ingress_forwarded"')),('③ 큐 처리 대기','sum(platform_kafka_committed_lag)'),('④ HTTP 접수',accepted)]):
 d.panel(name,expr,i*3,9,3,4,'stat','short' if i==2 else 'reqps',links=jump('infra' if i==2 else 'trace'))
for i,name in enumerate(['⑤ 결과 수신','⑥ 결과·폴백 처리','⑦ 최종화·정리','⑧ 고객 통지']):d.text(name,'**미구현**\n\n업무 연결 후 표시',12+i*3,9,3,4)
d.text('내부 채널 · 2차 발송','**현재 인입:** 인증 HTTP API\n\n**미연결:** 내부 업무 채널\n\n**미구현:** TCP 2차, 결과 웹훅, 만료·재시도, 고객 결과 통지\n\n과금 기능은 현재 요구 범위에 없습니다.',0,13,6,9)
d.panel('입력과 실제 접수 저장 처리량',api,6,13,18,9,unit='reqps',legend='API 입력')
d.panels[-1]['targets'].append({'refId':'B','expr':accepted,'legendFormat':'신규 업체 접수 저장','range':True})
d.panel('단계별 평균 처리 시간',f'sum by (stage) ({r(STAGE+"_sum")}) / sum by (stage) ({r(STAGE+"_count")})',0,22,12,8,unit='s')
d.panel('큐 적체 추이','sum by (topic) (platform_kafka_committed_lag)',12,22,12,8,legend='{{topic}}')
d.panel('가장 오래된 대기 시간','max by (topic) (platform_kafka_oldest_uncommitted_age_seconds)',0,30,12,8,unit='s',legend='{{topic}}')
d.panel('현재 경보 · pending / firing','ALERTS',12,30,12,8,'table',description='경보가 없으면 빈 표. 수집 상태 패널도 함께 확인')
d.panel('서비스 수집 상태','up{job=~"api|ingress|dispatch|simulator|kafka-observer"}',0,38,12,7,'table')
d.text('PPT 지표 중 아직 계산할 수 없는 항목','결과 웹훅 지연·미수신, TCP 폴백 건수, 최종 전송 성공률, 과금 미처리, 고객 전달 누락은 해당 업무 구현이 필요합니다.\n\nMTTD/MTTR은 장애 시작·감지·복구 이력이 필요하며 현재 uptime으로 대신 계산하지 않습니다. AWS A/C 존과 EKS는 미연결이고 현재 zone은 **local**입니다.',12,38,12,7)
d.logs('최근 처리·오류 관측',base_logs+' | json | outcome=~"http_error|transport_error|review_required|publish_unconfirmed"',0,45,24,9)
d.save()

d=Dashboard('services','서비스')
d.text('서비스별 처리량 · 지연 · 자원','PPT의 EKS 서비스 화면을 현재 실행 중인 앱 단위로 구성했습니다. CPU는 프로세스 사용률, JVM heap은 실제 사용량입니다. Pod/노드 재시작 횟수는 EKS 연결 전에는 표시하지 않습니다.',0,0)
d.panel('서비스 UP','up{job=~"api|ingress|dispatch|simulator"}',0,3,8,7,'table')
d.panel('앱 가동 시간','time() - process_start_time_seconds{application=~".+"}',8,3,8,7,'table','s')
d.panel('관측 구간 앱 재시작',f'changes(process_start_time_seconds{{application=~".+"}}[{RANGE}])',16,3,8,7,'table',description='스크레이프 사이 재시작은 놓칠 수 있음. Kubernetes restartCount가 아님')
d.panel('단계별 처리 TPS',f'sum by (stage,result) ({r(STAGE+"_count")})',0,10,12,8,unit='reqps',legend='{{stage}} · {{result}}')
d.panel('단계별 p95',quant(STAGE,by='stage'),12,10,12,8,unit='s')
d.panel('단계별 오류율',f'100 * sum by (stage) ({r(STAGE+"_count", "result=\"failure\"")}) / sum by (stage) ({r(STAGE+"_count")})',0,18,12,8,unit='percent')
d.panel('현재 처리 중','delivery_stage_active',12,18,12,8,legend='{{application}} · {{stage}}')
d.panel('프로세스 CPU','100 * process_cpu_usage',0,26,12,8,unit='percent',legend='{{application}}')
d.panel('JVM heap','sum by (application) (jvm_memory_used_bytes{area="heap"})',12,26,12,8,unit='bytes',legend='{{application}}')
d.panel('GC 정지 시간 비율',f'100 * sum by (application) ({r("jvm_gc_pause_seconds_sum")})',0,34,12,8,unit='percent',legend='{{application}}')
d.panel('Consumer별 commit 평균','kafka_consumer_coordinator_commit_latency_avg',12,34,12,8,unit='ms',legend='{{application}} · {{client_id}}')
d.panel('Consumer별 파티션 할당','kafka_consumer_coordinator_assigned_partitions',0,42,12,7,'table')
d.panel('HTTP API 응답 코드',f'sum by (status) ({r("http_server_requests_seconds_count", "application=\"delivery-api\",uri=\"/api/v1/deliveries\"")})',12,42,12,7,unit='reqps',legend='{{status}}')
p=d.panel('서비스 수집 상태 이력','up{job=~"api|ingress|dispatch|simulator"}',0,49,24,8,'state-timeline',legend='{{job}}')
p['fieldConfig']['defaults']['mappings']=[{'type':'value','options':{'0':{'text':'수집 실패','color':'red'},'1':{'text':'연결','color':'green'}}}]
p['options']={'showValue':'auto','mergeValues':True,'rowHeight':0.8,'legend':{'displayMode':'list','placement':'bottom'}}
d.save()

d=Dashboard('providers','외부 업체')
d.text('1차 HTTP 업체 · 시뮬레이터','현재 연결 업체는 **mock-provider** 하나입니다. SKT/KT/LGU 이름이나 실적을 임의로 만들지 않습니다. 실제 업체와 결과 웹훅이 연결되면 업체별 분해를 확장합니다.',0,0)
for i,(title,expr,unit) in enumerate([('HTTP 시도 TPS',total(STAGE+'_count','stage="dispatch_http"'),'reqps'),('접수 성공률',http_success,'percent'),('HTTP p95',quant(STAGE,'stage="dispatch_http"'),'s'),('운영 확인 관측',f'sum(increase(delivery_outcomes_total{{outcome="dispatch_review"}}[{RANGE}]))','short')]):d.panel(title,expr,i*6,3,6,4,'stat',unit)
d.panel('HTTP p50 / p95 / p99',quant(STAGE,'stage="dispatch_http"',.5),0,7,12,9,unit='s',legend='p50')
for key,q in [('B',.95),('C',.99)]:d.panels[-1]['targets'].append({'refId':key,'expr':quant(STAGE,'stage="dispatch_http"',q),'legendFormat':f'p{int(q*100)}','range':True})
d.panel('시뮬레이터 호출과 처리 효과',r('simulator_calls_total'),12,7,12,9,unit='reqps',legend='호출')
d.panels[-1]['targets'].append({'refId':'B','expr':r('simulator_effects_total'),'legendFormat':'처리 효과','range':True})
d.panel('내부 중복 차단',f'sum(increase(delivery_outcomes_total{{outcome="dispatch_duplicate"}}[{RANGE}]))',0,16,8,5,'stat')
d.panel('시뮬레이터 외부 중복 제거',f'sum(increase(simulator_deduplicated_total[{RANGE}]))',8,16,8,5,'stat')
d.panel('시뮬레이터 용량 거절',f'sum(increase(simulator_capacity_rejections_total[{RANGE}]))',16,16,8,5,'stat')
d.text('결과 수신 · 폴백 · 외부 멱등성','결과 웹훅 지연·미수신·TCP 2차 발송은 미구현입니다. 시뮬레이터 calls/effects 차이는 정상 성공률과 다르며 강제 실패도 함께 확인해야 합니다. Provider 성공 후 결과 저장 전 장애는 운영 확인 대상입니다.',0,21,24,3)
d.logs('업체 오류 및 운영 확인',base_logs+' | json | stage=~"provider|dispatch" | outcome!= "accepted"',0,24)
d.save()

d=Dashboard('customers','고객')
d.text('고객별 영향도 · 로그 기반','고객 ID는 Loki 로그 필드로만 저장하며 Prometheus label에는 추가하지 않습니다. 표의 건수는 **선택 구간의 로그 관측 횟수**로, 고유 발송 수·과금 수·유실 판정이 아닙니다. 미해결 상태 원장 조회와 고객 SLA는 후속 구현입니다.',0,0)
for i,(title,stage,outcome) in enumerate([('고객별 API 접수 관측 TOP 10','api','accepted'),('고객별 업체 접수 저장 관측 TOP 10','dispatch','accepted'),('고객별 중복 차단 관측 TOP 10','dispatch','duplicate'),('고객별 운영 확인 관측 TOP 10','dispatch','review_required')]):
 log_metric(d,title,f'topk(10, sum by (tenantId) (count_over_time({base_logs} | json tenantId="tenantId",stage="stage",outcome="outcome" | stage="{stage}" | outcome="{outcome}" [{RANGE}])))',(i%2)*12,3+(i//2)*8)
d.logs('선택 고객의 최근 처리 기록',base_logs+' | json | tenantId="$tenant"',0,19)
d.text('고객 SLA · 최종 결과','결과 지연 3분·폴백·최종 전달 실패·누락 등 PPT의 고객 SLA 항목은 아직 해당 상태를 계산할 원장이 없습니다. 현재 로그 건수나 API 오류를 최종 결과로 대체하지 않습니다.',0,29,24,3)
d.save()

d=Dashboard('errors','오류코드')
d.text('현재 코드 체계','현재는 mock-provider의 HTTP 상태 코드·전송 오류와 API Kafka 발행 오류를 표시합니다. PPT의 71010/79998 등 통신사 코드는 업체 프로토콜이 정해진 뒤 추가합니다. zone=local이며 A/C 존 구분은 미연결입니다.',0,0)
log_metric(d,'업체·단계·오류코드별 관측',f'sum by (provider,stage,code,outcome) (count_over_time({base_logs} | json provider="provider",stage="stage",code="code",outcome="outcome" | outcome=~"http_error|transport_error|publish_unconfirmed|review_required" [{RANGE}]))',0,3,24,9)
d.panel('API 응답 상태별 요청',f'sum by (status) (increase(http_server_requests_seconds_count{{application="delivery-api",uri="/api/v1/deliveries"}}[{RANGE}]))',0,12,12,8,'table')
d.panel('DB 일반 오류와 조건 불일치',f'sum by (operation,result) ({r("delivery_dynamodb_duration_seconds_count", "result!=\"success\"")})',12,12,12,8,unit='ops',legend='{{operation}} · {{result}}')
d.logs('최근 오류 관측',base_logs+' | json | outcome=~"http_error|transport_error|publish_unconfirmed|review_required"',0,20)
d.save()

d=Dashboard('infra','인프라')
d.text('Kafka · DynamoDB · Redis · PostgreSQL','로컬 Docker 환경입니다. MSK·MemoryDB·RDS·EKS 운영 지표로 해석하지 않습니다. DynamoDB는 앱의 SDK 계측만 읽고 모니터링 때문에 업무 테이블을 Scan하거나 기록을 추가하지 않습니다.',0,0)
d.panel('수집 대상 상태','up',0,3,12,8,'table')
d.panel('Kafka 관측 정상 여부','platform_kafka_probe_up',12,3,6,4,'stat',description='0이면 적체 수치를 숨김. 오래된 0을 정상으로 보이지 않게 처리')
d.panel('Kafka 마지막 관측 이후','time()-platform_kafka_probe_last_success_timestamp_seconds',18,3,6,4,'stat','s')
d.panel('Kafka 전체 처리 대기','sum(platform_kafka_committed_lag)',12,7,6,4,'stat')
d.panel('Kafka 최장 대기','max(platform_kafka_oldest_uncommitted_age_seconds)',18,7,6,4,'stat','s')
d.panel('토픽·파티션별 committed lag','platform_kafka_committed_lag',0,11,12,9,legend='{{topic}} · p{{partition}}')
d.panel('토픽별 Kafka 기록 증가율','sum by (topic) (clamp_min(deriv(platform_kafka_log_end_offset[2m]),0))',12,11,12,9,unit='ops',legend='{{topic}}',description='log end offset 기울기. reset/retention 영향 주의, 고유 업무 TPS가 아님')
d.panel('DynamoDB SDK 작업 횟수',f'sum by (operation,result) ({r("delivery_dynamodb_duration_seconds_count")})',0,20,12,9,unit='ops',legend='{{operation}} · {{result}}')
d.panel('DynamoDB 작업 p95',quant('delivery_dynamodb_duration_seconds',by='operation'),12,20,12,9,unit='s',legend='{{operation}}')
d.panel('정상 접수당 SDK 쓰기 비율',f'sum({r("delivery_dynamodb_duration_seconds_count", "operation=~\"put_item|update_item\",result=\"success\"")}) / ({accepted})',0,29,8,5,'stat',description='같은 시간창의 비율. 중복·진행 중 작업 때문에 정확한 건당 과금이 아님')
d.panel('Redis 메모리','redis_memory_used_bytes',8,29,8,5,'stat','bytes')
d.panel('PostgreSQL 연결 수','sum(pg_stat_database_numbackends{datname="delivery"})',16,29,8,5,'stat')
d.panel('Redis 명령 처리율',r('redis_commands_processed_total'),0,34,12,8,unit='ops',legend='Redis')
d.panel('Redis evicted keys',r('redis_evicted_keys_total'),12,34,12,8,unit='ops',legend='eviction')
d.panel('PostgreSQL commit / rollback',r('pg_stat_database_xact_commit','datname="delivery"'),0,42,12,8,unit='ops',legend='commit')
d.panels[-1]['targets'].append({'refId':'B','expr':r('pg_stat_database_xact_rollback','datname="delivery"'),'legendFormat':'rollback','range':True})
d.panel('DB exporter 상태','redis_up or pg_up',12,42,12,8,'table')
d.panel('Alloy 로그 전송 포기',f'sum by (reason) ({r("loki_write_dropped_entries_total")})',0,50,12,8,unit='ops',legend='{{reason}}',description='Loki/Alloy 자체 수집 상태와 함께 확인')
d.panel('현재 경보','ALERTS',12,50,12,8,'table')
d.panel('요청 제한 판단 결과',f'sum by (outcome) ({r("delivery_admission_duration_seconds_count")})',0,58,12,8,unit='reqps',legend='{{outcome}}')
d.panel('Redis 정책·제한 판단 p95',quant('delivery_admission_duration_seconds',by='outcome'),12,58,12,8,unit='s',legend='{{outcome}}',description='앱에서 측정한 전체 admission 판단 시간. Redis 서버 자체 latency가 아님')
d.save()

d=Dashboard('trace','메시지 추적')
d.text('메시지 단계별 상세 · Loki','상단에 deliveryId를 입력하면 해당 메시지의 API 접수·Ingress 인계·Dispatch 접수·중복 차단·오류를 조회합니다. 로그를 펼치면 tenantId, attemptId, provider, code가 보입니다.\n\n이 목록은 관측 로그이며 현재 DB 상태 목록이 아닙니다. 로그 부재를 발송 유실로 판정하지 않습니다. 본문·토큰·고객 메시지 내용은 기록하지 않습니다.',0,0,24,4)
d.logs('메시지 처리 기록',base_logs+' |= "$delivery" | json',0,4,24,18)
d.text('후속 단계','결과 웹훅·TCP 2차·만료·고객 통지 단계는 업무 구현 후 같은 ID로 연결합니다. 운영자가 실제 상태를 조회·조치하는 API는 별도 구현이 필요합니다.',0,22,24,3)
d.save()
print('Generated',len(PAGES),'dashboards')
