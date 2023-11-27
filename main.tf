variable "single_table_name" {
  type = string
  default = "distributed-locks"
}

resource "aws_dynamodb_table" "distributed-locks" {
  name           = var.single_table_name
  
  billing_mode     = "PAY_PER_REQUEST"
  hash_key       = "PK"
  range_key      = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  # before you have data disapear on you 
  # 
  #ttl {
  #  attribute_name = "TimeToExist"
  #  enabled        = false
  #}

  tags = {
    Name        = var.single_table_name
  }
}