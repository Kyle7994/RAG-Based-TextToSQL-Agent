#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8000/query/debug"

run_test() {
  local question="$1"
  echo
  echo "=================================================="
  echo "Q: $question"
  echo "--------------------------------------------------"
  curl -s -X POST "$BASE_URL" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg q "$question" '{question:$q}')" | jq
}

# 50-benchmark set

run_test "What is the total amount of paid orders?"
run_test "How many paid orders are there?"
run_test "What is the total amount of refunded orders?"
run_test "How many pending orders are there?"
run_test "How many cancelled orders are there?"
run_test "What is the total amount of all orders?"
run_test "How many users are there in total?"
run_test "How many VIP users are there?"
run_test "What is the average amount of paid orders?"
run_test "What is the maximum amount of paid orders?"
run_test "What is the earliest paid_at time among paid orders?"
run_test "What is the latest paid_at time among paid orders?"
run_test "Count users by country."
run_test "Count users by channel."
run_test "Group paid order total amount by country."
run_test "Count distinct paid users by country."
run_test "Count paid orders by day."
run_test "What is the total amount of paid orders created between 2026-03-23 and 2026-03-25 inclusive?"
run_test "How many paid orders were created on 2026-03-23?"
run_test "What is the total amount of refunded orders created on or after 2026-03-20?"
run_test "Which users have placed refunded orders?"
run_test "How many non-VIP users have placed paid orders?"
run_test "How many users have never placed an order?"
run_test "Which user has the highest total amount of paid orders?"
run_test "Which country has the most paid orders?"
run_test "Which channels have generated paid order revenue? Return channel and total revenue."
run_test "What is the total paid order amount for VIP users only?"
run_test "What is the total paid order amount for non-VIP users only?"
run_test "How many distinct countries have at least one paid order?"
run_test "How many units has each product sold?"
run_test "What is the sales amount for each product?"
run_test "How many units were sold in the Accessories category?"
run_test "What is the sales amount of the Accessories category?"
run_test "What is the sales amount of the Electronics category?"
run_test "Which products are included in order 114?"
run_test "Which order has the highest total_amount?"
run_test "Which product category generated the highest sales amount?"
run_test "Which product sold the most units?"
run_test "How many products have never appeared in any order_items row?"
run_test "List the products that have never been ordered."
run_test "What is the average unit_price across all order_items?"
run_test "What is the total subtotal of all order_items?"
run_test "Which categories have at least two units sold? Return category and total units."
run_test "How many paid orders include more than one order item?"
run_test "What is the total revenue contributed by orders that contain more than one order item?"
run_test "Who are the recent high-value users?"
run_test "What is the retention rate?"
run_test "Which channel is the best?"
run_test "Who are the churned users?"
run_test "Delete all cancelled orders."