1)
cd /solution
docker compose up -d --build  

export KUBECONFIG=/Users/mitya/.kube/config

2)Перейдите в каталог, в котором находится Dockerfile, 
и выполните команду docker build.
cd /solution/service_dds
cr.yandex/crpid905uqhhngffuo7h/dds_service
docker build . -t cr.yandex/crpid905uqhhngffuo7h/dds_service:v2023-05-04-r1
docker build --platform linux/amd64 . -t cr.yandex/crpid905uqhhngffuo7h/dds_service:v2023-05-04-r1

3)PUSH in REGISTRY
docker push cr.yandex/crpid905uqhhngffuo7h/dds_service:v2023-05-04-r1
cr.yandex/crpid905uqhhngffuo7h/dds_service:v2023-05-04-r1
4)Пропишите тег в values.yaml
tag: "v2023-05-04-r1" 

5)релиз 
cd /service_dds
helm upgrade --install dds-service app -n c08-dmitrij-ovsyanik




cdm-service
cr.yandex/crpuf5n4qpqjun1ihvg1/cdm_service
export KUBECONFIG=/Users/mitya/.kube/config

2)Перейдите в каталог, в котором находится Dockerfile, 
и выполните команду docker build.
cd /solution/service_cdm
docker build . -t cr.yandex/crpuf5n4qpqjun1ihvg1/cdm_service:vv2023-05-04-r1
docker build --platform linux/amd64 . -t cr.yandex/crpuf5n4qpqjun1ihvg1/cdm_service:v2023-05-04-r1

3)PUSH in REGISTRY
docker push cr.yandex/crpuf5n4qpqjun1ihvg1/cdm_service:v2023-05-04-r1

4)Пропишите тег в values.yaml
tag: "v2023-05-04-r1" 

5)релиз 
cd /service_cdm
helm upgrade --install cdm-service app -n c08-dmitrij-ovsyanik


kubectl describe pod dds-service-5d7bc94d48-t8x6x
kubectl logs dds-service-5d7bc94d48-t8x6x dds-service
kubectl delete deployment dds-service
helm list
helm uninstall stg-service

