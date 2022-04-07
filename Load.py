from io import StringIO, BytesIO
import pandas as pd
class Load:
    def write_df_to_s3(self,trg_bucket, key, df_all, s3):
        out_buffer = BytesIO()
        df_all.to_parquet(out_buffer, index=False)
        bucket_target = s3.Bucket(trg_bucket)
        bucket_target.put_object(Body=out_buffer.getvalue(), Key=key)
        return bucket_target

    def load(self,bucket_target):
    
        objKey=[]
        for obj in bucket_target.objects.all():
            objKey.append(obj.key)
        prq_obj = bucket_target.Object(key=objKey[-1]).get().get('Body').read()
        data = BytesIO(prq_obj)
        return data

    def etl_report(self,bucket_target):
        df_report = pd.read_parquet(self.load(bucket_target))
        return df_report