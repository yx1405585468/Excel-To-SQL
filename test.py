import pandas as pd

pd.set_option("display.width", None)
pd.set_option("display.max_rows", None)
if __name__ == '__main__':
    # data = pd.read_excel("C:/Users/29688/Desktop/测试数据.xlsx").drop(0).reset_index(drop=True)
    # data.to_csv("C:/Users/29688/Desktop/测试数据.csv", index=False)
    data=pd.read_csv(r"C:\Users\29688\Desktop\cs1011.csv",encoding="utf-8")
    print(data)
