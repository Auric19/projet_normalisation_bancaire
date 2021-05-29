from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def applyRules(normalized_payment, db_properties):
    df_amount_bank_month = amountByBankByMonth(normalized_payment)
    df_amount_bank_month.write.jdbc(url=db_properties['url'], table="amount_bank_month", mode='overwrite', properties=db_properties)

    df_numb_payment_bank_month = numbPaymentByBankByMonth(normalized_payment)
    df_numb_payment_bank_month.write.jdbc(url=db_properties['url'], table="numb_payment_bank_month", mode='overwrite', properties=db_properties)

    df_mean_amount_currency = meanAmountByCurrency(normalized_payment)
    df_mean_amount_currency.write.jdbc(url=db_properties['url'], table="mean_amount_currency", mode='overwrite', properties=db_properties)

    df_numb_rejected_payment_bank = numbRejectedPaymentByBank(normalized_payment)
    df_numb_rejected_payment_bank.write.jdbc(url=db_properties['url'], table="numb_rejected_payment_bank", mode='overwrite', properties=db_properties)


# Somme des montants par banque par mois
def amountByBankByMonth(df):
    df_amount_bank_month = df.withColumn("amount", df["amount"].cast(IntegerType()))\
        .groupBy("orderingBankName", "month").agg(sum("amount").alias("sumAmount"))
    df_amount_bank_month.show(10)
    return df_amount_bank_month


# Calcul du nombre de transaction par banque par mois
def numbPaymentByBankByMonth(df):
    df_numb_payment_bank_month = df.groupBy("orderingBankName", "month").count()
    df_numb_payment_bank_month.show(10)
    return df_numb_payment_bank_month


# Moyenne des montants transférés par devise
def meanAmountByCurrency(df):
    df_mean_amount_currency = df.withColumn("amount", df["amount"].cast(IntegerType()))\
        .groupBy("currency").agg(mean("amount").alias("averageAmount"))

    return df_mean_amount_currency


# Nombre de paiements rejetés par banque
def numbRejectedPaymentByBank(df):
    df_numb_rejected_payment_bank = df.filter(col("status") == "Rejected").groupBy("orderingBankName", "status").count()\

    #df_rule4 = df.groupBy("orderingBankName", "status").count()\
    #.where(col("status") == "Rejected")

    #df_numb_rejected_payment_bank.explain(extended=True)

    return df_numb_rejected_payment_bank
