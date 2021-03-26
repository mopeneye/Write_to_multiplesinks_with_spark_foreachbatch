# Write_to_multiplesinks_with_spark_foreachbatch
Writes streaming data to hive_postgres and delta table in with conditions.

Question is below:

`https://github.com/erkansirin78/datasets/raw/master/iot_telemetry_data.csv.zip` veri setini kullanarak aşağıda belirtilen işleri yapınız.

- 1.1. Veri setini `~/data-generator/output` klasörüne stream ediniz.

- 1.2. Veri kaynağında üç farklı sensöre ait veriler karışık olarak loglanmaktadır. Spark Structured Streaming kullanarak üç farklı sensörden gelen sinyalleri ayırarak üç farklı sink'e yazmanız bekleniyor. Bunlar;
    - `00:0f:00:70:91:0a` id numaralı sensöre ait veriler postgresql traindb `sensor_0a` tablosu
    - `b8:27:eb:bf:9d:51` id numaralı sensöre ait veriler hive test1 `sensor_51` tablosu
    - `1c:bf:ce:15:ec:4d` id numaralı sensöre ait veriler delta tablosu olarak `sensor_4d` tablosu

- 1.3. 250 satırdan sonra akışı durdurunuz. Üç farklı sinke de ayrı ayrı giderek sonuçları kontrol ediniz. İlgili sensöre ait veriler sadece yukarıda belirtilen sinkte bulunmalıdır, farklı sensöre ait veri olmamalıdır. 
