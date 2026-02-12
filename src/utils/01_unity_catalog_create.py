# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS shopmetrics_ecommerce;
# MAGIC CREATE SCHEMA IF NOT EXISTS shopmetrics_ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS shopmetrics_ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS shopmetrics_ecommerce.gold;
