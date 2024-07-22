![logo](/dataeng10_finalproject.png)

Proje adımları ile ilgili detaylı bilgi için lütfen öncelikle Medium makaleme göz atınız. 
Link: https://medium.com/@myoztiryaki/airflow-apache-spark-minio-ve-delta-lake-ile-etl-proje-ad%C4%B1mlar%C4%B1-f3bac4f846b6

Bu projede, TMDB film veri setini kullanarak bir data pipline oluşturulması istenmektedir. Proje kapsamında gerçekleştirilecek adımlar ve kullanılacak teknolojiler şunlardır:

Veri Alımı:
TMDB veri setleri düzenli aralıklarla "tmdb-bronze" adlı bir MinIO bucket'ına yazılacaktır. Bu aşamada veri üretici (data-generator) kullanılacaktır.

Veri İşleme ve Dönüşüm:
MinIO'da saklanan veriler Apache Spark kullanılarak işlenecek, JSON ve diğer kompleks yapıdaki sütunlar ayrıştırılacaktır.
İşlenmiş veriler belirlenen şemalarda MinIO'ya Delta Lake formatında yazılacaktır.

Otomatikleştirilmiş pipline:
Tüm bu süreçlerin günlük olarak çalışması için Apache Airflow kullanılarak bir DAG (Directed Acyclic Graph) oluşturulacaktır.

Bu projenin amacı, ham veri setlerini işleyerek SQL diliyle kolayca sorgulanabilir hale getirmek ve veri analistlerinin ihtiyaç duyduğu gelişmiş analizleri gerçekleştirmelerini sağlamaktır. Projede kullanılacak başlıca teknolojiler MinIO, Apache Spark, Delta Lake ve Apache Airflow'dur.

Şemada gösterildiği gibi, veri işleme adımları veri üreticiden MinIO'ya yazma, MinIO'dan Apache Spark ile veri işleme ve sonunda Delta Lake formatında MinIO'ya yazma şeklinde olacaktır.
Tüm bu işlemler Docker Compose ile konteynerlar üzerinde çalıştırılacaktır.

Eğer dosyalarda sorun çıkarsa izinleri kontrol ediniz. 
sudo chmod 777 movies_to_s3.py
sudo chmod 777 credits_to_s3.py
sudo chmod 777 schema_movies.py
sudo chmod 777 schema_credits.py

sudo chown ssh_train:root credits_to_s3.py
sudo chown ssh_train:root movies_to_s3.py
sudo chown ssh_train:root schema_movies.py
sudo chown ssh_train:root schema_credits.py

sorular için jupyter kısmını açma
docker exec -it spark_client bash
cd /dataops
source airflowenv/bin/activate

jupyter lab --ip 0.0.0.0 --port 8888 --allow-root
