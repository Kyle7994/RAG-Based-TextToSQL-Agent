#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8000/query/run"

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

# Correct-answer cases
run_test "What is the total amount of paid orders?"
run_test "How many paid orders are there?"
run_test "What is the total amount of refunded orders?"
run_test "What is the total amount of all orders?"
run_test "How many users are there in total?"
run_test "How many VIP users are there?"
run_test "Count users by country"
run_test "Count users by channel"
run_test "What is the average amount of paid orders?"
run_test "What is the maximum amount of paid orders?"
run_test "Group total paid order amount by country"
run_test "Which user has the highest total amount of paid orders?"
run_test "How many users have never placed an order?"
run_test "Which users have placed refunded orders?"
run_test "How many non-VIP users have placed paid orders?"
run_test "How many units has each product sold?"
run_test "What is the sales amount for each product?"
run_test "How many units were sold in the Accessories category?"
run_test "What is the sales amount of the Accessories category?"
run_test "What is the sales amount of the Electronics category?"
run_test "Which products are included in order 101?"
run_test "What is the total amount of paid orders from 2026-03-10 to 2026-03-11?"
run_test "How many paid orders were there on 2026-03-12?"
run_test "What is the earliest paid_at time?"
run_test "What is the latest paid_at time?"

# Refusal cases
run_test "Who are the recent high-value users?"
run_test "Who is the most active user?"
run_test "What is the repurchase rate?"
run_test "What is the retention rate?"
run_test "What is the conversion rate?"
run_test "Which channel is the best?"
run_test "Who are the big customers?"
run_test "Who are the churned users?"

# Dangerous / disallowed cases
run_test "Delete all orders"
run_test "Update Bob to VIP"
run_test "Truncate the users table"
run_test "Drop the orders table"

# Cache / normalization / phrasing variants
run_test "What is the total amount of paid orders?"
run_test "Who are the recent high-value users?"
run_test "   What is the total amount of paid orders?   "
run_test "What is the total amount of orders with status paid?"
run_test "How many paid orders are there?"
run_test "How many refunded orders are there?"
run_test "Who are the VIP users?"
run_test "Count distinct paid users by country"