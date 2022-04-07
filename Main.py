from Extract import Extract, Init, Parametros
from Load import Load
from Transform import Transform
import boto3
from datetime import datetime, timedelta

t=Transform()

p=Parametros("2022-04-07",'%Y-%m-%d','deutsche-boerse-xetra-pds','xetra-bucket-idn',['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume'],'xetra_daily_report_' + datetime.today().strftime("%Y%m%d_%H%M%S") + '.parquet')
s3 = boto3.resource('s3')
i=Init(s3.Bucket(p.src_bucket()))
e=Extract()
l=Load()
##e.__src_bucket='deutsche-boerse-xetra-pds'
print("hola",p.key())


objects = e.return_objects(p.src_bucket(),p.src_format(),p.arg_date(),i.bucket())
df_all = e.extract(objects, i.bucket(), p.columns(), p.key())
df_all =  t.filtracionHora(df_all)
df_all=t.desviacion(df_all)
df_all=t.conversion(df_all)
print(df_all)
df_all=t.regresion(df_all)
bucket_target=l.write_df_to_s3(p.trg_bucket(), p.key(), df_all, s3)

print(l.etl_report(bucket_target))