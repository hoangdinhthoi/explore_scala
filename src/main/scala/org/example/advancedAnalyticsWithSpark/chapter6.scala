package org.example.advancedAnalyticsWithSpark

// Analyzing Co-occurrence Networks with GraphX

import org.apache.spark.SparkContext
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.conf.Configuration


object chapter6 {
  println("OK")
}


/**
 * 0. Business understanding, get to know the business model, the current painpoint
 * 1. EDA: Understand the data, work closely with domain expert,
 * distribution of each field, clean data (by age & opt out)
 * 2. Mo hinh hoa bai toan: supervised learning, pattern, dinh nghia label
 * 3. Enrich data. Loop through data in the past.
 * 4. Feature extraction, fill missing values, highly unbalanced data
 * 5. Build base line model, dinh nghia evaluation metric
 * 6. Improvement: feature selection (by correlation, importance by random forest), model tuning
 * Conversion rate = Recall / Detection rate = Precision
 *
 * 1. Xin chữ kí các giấy tờ, scan bản mềm để gửi cho nc.thuc trước, bản cứng gửi về nhà máy cho nc.thuc sau
 * 2. Bàn giao tài sản với vt.huong5
 * 3. Gửi số BHXH cho c Ngà để chốt
 *
 */