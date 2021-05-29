from pyspark.sql.functions import *


def run(payment_data):
    df_with_date = processDate(payment_data)
    df_with_bank_code = getBankCode(df_with_date)
    df_with_bank_country = getBankCountry(df_with_bank_code)
    df_with_status = getPaymentStatus(df_with_bank_country)
    df_with_currency = getCurrency(df_with_status)
    df_with_currency.cache()
    df_with_currency.show(50)

    return df_with_currency


def processDate(df):
    df_with_date = df.withColumn('timestamp', unix_timestamp((col('timestamp') / 1000).cast("timestamp"))) \
        .withColumn('date', to_date(from_unixtime(col("timestamp")))) \
        .withColumn('month', from_unixtime(col("timestamp"), "yyyy-MM")) \
        .withColumn('year', from_unixtime(col("timestamp"), "yyyy"))

    return df_with_date


def getBankCode(df):
    df_with_bank_code = df.withColumn('orderingBankCode', df['orderingBankBIC'].substr(7, 2)) \
        .withColumn('receivingBankCode', df['receivingBankBIC'].substr(7, 2))

    return df_with_bank_code


def getBankCountry(df):
    df_with_bank_country = df.withColumn('orderingBankCountry', df['orderingBankBIC'].substr(5, 2)) \
        .withColumn('receivingBankCountry', df['receivingBankBIC'].substr(5, 2))

    return df_with_bank_country


def getPaymentStatus(df):
    df_with_status = df.withColumn('status', (when(col("codeStatus") == '01', 'Pending')
                                              .when(col("codeStatus") == '02', 'Sent')
                                              .when(col("codeStatus") == '03', 'Acknowledged')
                                              .when(col("codeStatus") == '04', 'Rejected')))

    return df_with_status


def getCurrency(df):
    df_with_currency = df.withColumn('currency', (when(col("orderingBankCountry") == 'US', 'USD')
                                                  .when(col("receivingBankCountry") == 'US', 'USD')
                                                  .otherwise('EUR')))

    return df_with_currency
