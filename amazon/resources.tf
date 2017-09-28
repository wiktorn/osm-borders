provider "aws" {
  region = "eu-west-1"
}

data "aws_iam_policy_document" "osm_apps_lambda_policy_document_ro" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:DescribeTable",
    ]
    resources = [
      "${module.osm_prg_gminy_dict.dynamo_table_arn}",
      "${module.osm_prg_wojewodztwa_dict.dynamo_table_arn}",
      "${module.osm_cache_meta_dict.dynamo_table_arn}",
    ]
  }
}

data "aws_iam_policy_document" "osm_apps_lambda_policy_document_rw" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:BatchWriteItem",
      "dynamodb:CreateTable",
      "dynamodb:DeleteTable",
      "dynamodb:DescribeTable",
      "dynamodb:GetItem",
      "dynamodb:PutItem",
    ]
    resources = [
      "${module.osm_prg_gminy_dict.dynamo_table_arn}",
      "${module.osm_prg_wojewodztwa_dict.dynamo_table_arn}",
      "${module.osm_cache_meta_dict.dynamo_table_arn}",
    ]
  }
}

data "aws_iam_policy_document" "lambda_policy" {
    statement {
      actions = ["sts:AssumeRole"],
      principals {
        identifiers = ["lambda.amazonaws.com", "apigateway.amazonaws.com"]
        type="Service"
      }
      effect = "Allow"
    }
}

resource "aws_s3_bucket" "lambda_repo" {
  bucket = "vink.pl.lambdacode"
  acl = "private"
}

resource "aws_s3_bucket_object" "osm_borders_package" {
  bucket = "${aws_s3_bucket.lambda_repo.id}"
  key = "osm_borders_package"
  source = "../package.zip"
  etag = "${md5(file("../package.zip"))}"
  depends_on = ["null_resource.build_lambda"]
}

resource "aws_iam_role" "osm_apps_lambda_iam" {
  name = "osm_apps_lambda_iam"
  assume_role_policy = "${data.aws_iam_policy_document.lambda_policy.json}"
}

resource "aws_iam_role_policy" "osm_apps_lambda_iam_dynamo_ro" {
  role = "${aws_iam_role.osm_apps_lambda_iam.id}"
  policy = "${data.aws_iam_policy_document.osm_apps_lambda_policy_document_ro.json}"
}

resource "aws_iam_user" "osm_borders_dynamo_rw" {
  name = "osm_borders_lambda"
  path = "/osm/osm-borders/"
}

resource "aws_iam_user_policy" "osm_borders_dynamo_rw_policy" {
  name = "osm_borders_dynamo_rw_policy"
  user = "${aws_iam_user.osm_borders_dynamo_rw.name}"
  policy = "${data.aws_iam_policy_document.osm_apps_lambda_policy_document_rw.json}"
}

resource "aws_iam_access_key" "osm_borders_lambda_access_key" {
  user    = "${aws_iam_user.osm_borders_dynamo_rw.name}"
}

/*
  access key is here:
  access_key = ${aws_iam_access_key.osm_borders_lambda_access_key.id}
  secret = ${aws_iam_access_key.osm_borders_lambda_access_key.secret}
*/

resource "aws_lambda_function" "osm_borders_lambda" {
/*  filename          = "../package.zip"*/
  s3_bucket = "${aws_s3_bucket_object.osm_borders_package.bucket}"
  s3_key = "${aws_s3_bucket_object.osm_borders_package.key}"
  function_name     = "osm-borders"
  role              = "${aws_iam_role.osm_apps_lambda_iam.arn}"

  handler           = "rest_server.app"
  source_code_hash  = "${base64sha256(file("../package.zip"))}"
  memory_size		= 128
  runtime           = "python3.6"

  environment {
    variables {
      foo   = "bar"
    }
  }
  depends_on = ["null_resource.build_lambda"]
}

resource "aws_lambda_permission" "osm_borders_lambda_permissions" {
  action = "lambda:invokeFunction"
  principal = "apigateway.amazonaws.com"
  function_name = "${aws_lambda_function.osm_borders_lambda.function_name}"
  statement_id = "AllowExecutionFromAPIGateway"
}

resource "aws_api_gateway_rest_api" "osm_borders_api" {
  name = "osm_borders"
}

resource "aws_api_gateway_resource" "osm_borders_api_resource" {
  parent_id = "${aws_api_gateway_rest_api.osm_borders_api.root_resource_id}"
  path_part = "osm-borders"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
}

resource "aws_api_gateway_resource" "osm_borders_terc_api_resource" {
  parent_id = "${aws_api_gateway_resource.osm_borders_api_resource.id}"
  path_part = "{terc}"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
}


resource "aws_api_gateway_method" "osm_borders_api_method" {
  authorization = "NONE"
  http_method = "GET"
  resource_id = "${aws_api_gateway_resource.osm_borders_terc_api_resource.id}"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
}

resource "aws_api_gateway_integration" "osm_borders_api_integration" {
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
  resource_id = "${aws_api_gateway_resource.osm_borders_terc_api_resource.id}"
  http_method = "${aws_api_gateway_method.osm_borders_api_method.http_method}"
  integration_http_method = "POST"
  type ="AWS_PROXY"
  uri = "${aws_lambda_function.osm_borders_lambda.invoke_arn}"
}

resource "aws_api_gateway_deployment" "osm_borders_deployment" {
  rest_api_id = "${aws_api_gateway_integration.osm_borders_api_integration.rest_api_id}"
  stage_name = "api"
  stage_description = "${timestamp()}" // workaround: terraform issue #6613?
}

output "url" {
  value = "${aws_api_gateway_deployment.osm_borders_deployment.invoke_url}"
}

module "osm_prg_gminy_dict" {
  source = "./dynamo_cache"
  name = "osm_prg_gminy_v1"
}

module "osm_prg_wojewodztwa_dict" {
  source = "./dynamo_cache"
  name = "osm_prg_wojewodztwa_v1"
}

module "osm_cache_meta_dict" {
  source = "./dynamo_cache"
  name = "meta"
}

resource "null_resource" "build_lambda" {
  provisioner "local-exec" {
    command = "cd .. && ./build_package.sh"
  }
}
