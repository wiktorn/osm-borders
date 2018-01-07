/*  add autoscaling later */
variable "name" {}

output "dynamo_ouput_cache_arn" {
  value = "${aws_dynamodb_table.table.arn}"
}

output "name" {
  value = "${var.name}"
}

resource  "aws_dynamodb_table" "table" {
  name = "${var.name}"
  read_capacity = 1
  write_capacity = 1
  hash_key = "key"
  attribute {
    name = "key"
    type = "S"
  }
}

