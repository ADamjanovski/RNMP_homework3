Домашна задача број 3 - РНМП

Го креираме најдобриот модел со:

python create_model.py

За трите модели ги избрав LogicistigRegression, MLPClassifier и DecisionTreeClasifier. F1 score се движеше околу 0.3 вредноста. Како најдобар најчесто се издвојуваше DecisionTreeClasifier. Множеството го поделив со train_test_split и random_state=42 за да може истата поделба да се реплецира и во producer.py и да немаме data leakage. Во utils.py iимаме две функции кои ги користиме за да отстранеме не потребни колони од датасетот и да транформираме две колони во една.

Во producer.py ги испраќам податоците во JSON формат на околу  0.5 до 2 секунди. 

Ја стартуваме со producer.sh

Во consumer.py конзумираме од топикот health_data_predicted и го печатиме json-от. Ја стартуваме со consume.sh

Главниот дел од апликацијата се извршува во spark.predictor.py. Креираме spark сесија која конзумира од топикот health_data и правиме writeStream кој за секој batch од податоци кои стигаат спарк ги добива ја извршува функцијата process_batch. Во process_batch прво ги извршуваме истите транформации од utils.py, ги претвараме во pandas Dataframe и со лоадираниот модел правиме предикција дали има диабетис или не. Потоа го додаваме нашето предвидување како нова колона во Dataframe-от и во json формат го испраќаме на топикот health_data_predicted