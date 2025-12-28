Домашна задача број 3 - РНМП

Го креираме најдобриот модел со:

python create_model.py

За трите модели ги избрав LogicistigRegression, MLPClassifier и DecisionTreeClasifier. F1 score се движеше околу 0.3 вредноста. Како најдобар најчесто се издвојуваше DecisionTreeClasifier. Множеството го поделив со train_test_split и random_state=42 за да може истата поделба да се реплецира и во producer.py и да немаме data leakage. 

Во producer.py ги испраќам податоците во JSON формат на околу  0.5 до 2 секунди. 

Ја стартуваме со producer.sh

