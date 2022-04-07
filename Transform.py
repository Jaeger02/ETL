import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pyplot
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
class Transform:

    def filtracionHora(self, df_all):
        df_all= df_all.loc[(df_all["Time"] >= '08:00') & (df_all["Time"]<='12:00') , ["ISIN", "Date","Time","StartPrice","EndPrice"]]
        return df_all
    def desviacion(self,df_all):
        stdStart= df_all["StartPrice"].std()
        stdEnd= df_all["EndPrice"].std()

        stdAgrupada = (((178702-1)*(stdStart**2)) + ((178702-1)*(stdEnd**2))) / (178702+178702-2)
        stdAgrupadaTotal = stdAgrupada ** (0.5)
        ##df_all['StartPrice_std'] = stdStart
        ##df_all['EndPrice_std'] = stdEnd
        df_all['Start-EndPrice_STD'] = stdAgrupadaTotal
        return df_all
    def conversion(self, df_all):
        df_all["EndPrice_MXN"]= df_all["EndPrice"] * 22.93
        df_all["ValorPred_Endprice"]= 9.03802772e+01
        return df_all

    def regresion(self,df_all):
        y = df_all['EndPrice']
        X = df_all['Time'].replace({':':'.'}, regex=True).astype(float)
       
        plt.plot(X,y, color="red")
        plt.grid(alpha=0.3)
        y=y.reset_index().values
        X=X.reset_index().values 

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
        regr = linear_model.LinearRegression().fit(X_train,y_train)
        y_pred = regr.predict(X_test)
        ##print("Coeficientes: \n", regr.coef_)
        ##print("Error cuadratico medio: %.2f" % mean_squared_error(y_test, y_pred))
        # El coeficiente de determinacion: 1 de prediccion en perfecto
        ##print("Coeficinete de determinacion: %.2f" % r2_score(y_test,y_pred))
        plt.scatter(X_test, y_test, color="blue")
        plt.plot(X_test,y_pred, color="red")

        plt.grid(alpha=0.3)
        ##plt.savefig("Figure_1.png")
        plt.show()
        
        
        return True




