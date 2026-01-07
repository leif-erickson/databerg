# monitor.py
import os
import time
import requests
import docker
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from sqlmodel import Session, select, create_engine
from models.metrics import Metric
from database import engine

PROMETHEUS_URL = 'http://prometheus:9090'
SERVICES_TO_MONITOR = ['fastapi']
SLA_RESPONSE_TIME = 0.5
UPTIME_TARGET = 99.9
SCALE_UP_THRESHOLD_FACTOR = 1.2
SCALE_DOWN_THRESHOLD_FACTOR = 0.8
MIN_REPLICAS = 1
MAX_REPLICAS = 10
ALERT_WEBHOOK = os.environ.get('ALERT_WEBHOOK', '')

client = docker.from_env()

def query_prometheus(query):
    response = requests.get(f'{PROMETHEUS_URL}/api/v1/query', params={'query': query})
    return response.json()['data']['result']

def get_historical_metrics(service, metric='response_time'):
    with Session(engine) as session:
        metrics = session.exec(select(Metric).where(Metric.endpoint.like(f'/{service}%')).order_by(Metric.timestamp)).all()
    df = pd.DataFrame([{'timestamp': m.timestamp, 'value': m.response_time if metric == 'response_time' else m.status_code} for m in metrics])
    return df

def learn_baseline(df):
    model = ARIMA(df['value'], order=(5,1,0))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=1)
    return forecast[0]

def calculate_uptime():
    with Session(engine) as session:
        total = session.exec(select(Metric)).count()
        success = session.exec(select(Metric).where(Metric.status_code == 200)).count()
    return (success / total * 100) if total else 100

def check_container_states():
    services = client.services.list()
    for svc in services:
        if 'unhealthy' in svc.attrs['Tasks']:
            svc.update(mode={'Replicated': {'Replicas': svc.attrs['Mode']['Replicated']['Replicas'] + 1}})

def send_alert(message):
    if 'slack' in ALERT_WEBHOOK:
        requests.post(ALERT_WEBHOOK, json={'text': message})
    elif 'discord' in ALERT_WEBHOOK:
        requests.post(ALERT_WEBHOOK, json={'content': message})

def main():
    while True:
        for service in SERVICES_TO_MONITOR:
            current_metrics = query_prometheus(f'container_cpu_usage_seconds_total{{name=~"{service}.*"}}')
            current_cpu = float(current_metrics[0]['value'][1]) if current_metrics else 0
            
            errors = query_prometheus(f'rate(http_requests_total{{status=~"5..", job="{service}"}}[5m])')
            error_rate = float(errors[0]['value'][1]) if errors else 0
            
            df = get_historical_metrics(service)
            if len(df) > 10:
                baseline = learn_baseline(df)
                if current_cpu > baseline * SCALE_UP_THRESHOLD_FACTOR or error_rate > 0.01:
                    svc = client.services.get(service)
                    replicas = svc.attrs['Mode']['Replicated']['Replicas']
                    if replicas < MAX_REPLICAS:
                        svc.scale(replicas + 1)
                    send_alert(f"Scaling up {service} due to high load")
                elif current_cpu < baseline * SCALE_DOWN_THRESHOLD_FACTOR:
                    svc = client.services.get(service)
                    replicas = svc.attrs['Mode']['Replicated']['Replicas']
                    if replicas > MIN_REPLICAS:
                        svc.scale(replicas - 1)
            
            avg_response = df['value'].mean() if not df.empty else 0
            if avg_response > SLA_RESPONSE_TIME:
                send_alert(f"SLA breach for {service}: response time {avg_response}")
            
            uptime = calculate_uptime()
            if uptime < UPTIME_TARGET:
                send_alert(f"Uptime below target for {service}: {uptime}%")
        
        check_container_states()
        
        time.sleep(60)

if __name__ == '__main__':
    main()