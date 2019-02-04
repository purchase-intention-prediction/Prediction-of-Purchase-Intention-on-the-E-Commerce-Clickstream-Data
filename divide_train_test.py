with open('remainAttributes.csv','r',encoding="utf-8") as fp:
    fp.readline()
    count = 0
    for line in fp:
        count = count + 1

percent = round(count/50)

with open('remainAttributes.csv','r', encoding="utf-8") as fp:
    fp.readline()
    for i in range(2000000):
        fp.readline()
    with open('train.csv','w',encoding="utf-8") as train:
        with open('test.csv','w',encoding="utf-8") as test:
            for j in range(percent):
                for i in range(4):
                    train.write(fp.readline())
                for i in range(1):
                    test.write(fp.readline())