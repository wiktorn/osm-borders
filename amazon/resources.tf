provider "aws" {
  region = "eu-west-1"
}

data "aws_iam_policy_document" "osm_apps_lambda_policy_document_ro" {
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
      "dynamodb:DescribeTable",
      "dynamodb:PutItem",
      "dynamodb:Scan",
    ]
    resources = [
      "${module.osm_prg_gminy_dict.dynamo_table_arn}",
      "${module.osm_prg_wojewodztwa_dict.dynamo_table_arn}",
      "${module.osm_cache_meta_dict.dynamo_table_arn}",
      "${module.osm_teryt_wmrodz_dict.dynamo_table_arn}",
      "${module.osm_teryt_teryt_dict.dynamo_table_arn}",
      "${module.osm_teryt_simc_dict.dynamo_table_arn}",
      "${module.osm_teryt_ulic_dict.dynamo_table_arn}",
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:DescribeTable",
      "dynamodb:Scan",
      "dynamodb:PutItem",
    ]
    resources = [
      "${module.osm_prg_gminy_cache.dynamo_ouput_cache_arn}",
    ]
  }

  statement {
    effect = "Allow"
    actions =[
      "sns:Publish"
    ]
    resources = [
      "${aws_sns_topic.osm_api_osm_borders_topic.arn}"
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

resource "aws_s3_bucket_object" "osm_borders_package_rest" {
  bucket = "${aws_s3_bucket.lambda_repo.id}"
  key = "osm_borders_package_rest"
  source = "../package_rest.zip"
  etag = "${md5(file("../package_rest.zip"))}"
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

data "aws_iam_policy_document" "allow_tracing" {
    statement {
      actions = [
        "logs:PutLogEvents",
        "logs:CreateLogStream",
        "logs:CreateLogGroup",
        "logs:DescribeLogStreams"
      ],
      resources = ["arn:aws:logs:*:*:*"],
      effect = "Allow"
    }
}
resource "aws_iam_role_policy" "osm_apps_lambda_iam_trace" {
  role = "${aws_iam_role.osm_apps_lambda_iam.id}"
  policy = "${data.aws_iam_policy_document.allow_tracing.json}"
}


resource "aws_lambda_function" "osm_borders_lambda_rest" {
  s3_bucket = "${aws_s3_bucket_object.osm_borders_package_rest.bucket}"
  s3_key = "${aws_s3_bucket_object.osm_borders_package_rest.key}"
  function_name     = "osm-borders-rest"
  role              = "${aws_iam_role.osm_apps_lambda_iam.arn}"

  handler           = "rest_endpoint.app"
  source_code_hash  = "${base64sha256(file("../package_rest.zip"))}"
  memory_size		= 128
  runtime           = "python3.6"
  timeout = 300

  environment {
    variables {
      OSM_BORDERS_CACHE_TABLE = "${module.osm_prg_gminy_cache.name}"
      OSM_BORDERS_SNS_TOPIC_ARN = "${aws_sns_topic.osm_api_osm_borders_topic.arn}"
      USE_AWS = "true"
    }
  }
  depends_on = ["null_resource.build_lambda"]
}

resource "aws_lambda_permission" "osm_borders_lambda_permissions" {
  action = "lambda:invokeFunction"
  principal = "apigateway.amazonaws.com"
  function_name = "${aws_lambda_function.osm_borders_lambda_rest.function_name}"
  statement_id = "AllowExecutionFromAPIGateway"
}

resource "aws_lambda_permission" "osm_borders_sns_exec_fetcher_permission" {
  action = "lambda:invokeFunction"
  principal = "sns.amazonaws.com"
  function_name = "${aws_lambda_function.osm_borders_lambda_fetcher.function_name}"
  statement_id = "AllowExecutionFromSNS"
  source_arn = "${aws_sns_topic.osm_api_osm_borders_topic.arn}"
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

resource "aws_api_gateway_resource" "osm_borders_get_api_resource" {
  parent_id = "${aws_api_gateway_resource.osm_borders_api_resource.id}"
  path_part = "get"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
}

resource "aws_api_gateway_resource" "osm_borders_get_rest_api_resource" {
  parent_id = "${aws_api_gateway_resource.osm_borders_get_api_resource.id}"
  path_part = "{proxy+}"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
}


resource "aws_api_gateway_method" "osm_borders_api_method" {
  authorization = "NONE"
  http_method = "GET"
  resource_id = "${aws_api_gateway_resource.osm_borders_get_rest_api_resource.id}"
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
  request_parameters {
    "method.request.header.host" = true
  }
}

resource "aws_api_gateway_integration" "osm_borders_api_integration" {
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
  resource_id = "${aws_api_gateway_resource.osm_borders_get_rest_api_resource.id}"
  http_method = "${aws_api_gateway_method.osm_borders_api_method.http_method}"
  integration_http_method = "POST"
  type ="AWS_PROXY"
  uri = "${aws_lambda_function.osm_borders_lambda_rest.invoke_arn}"
  request_parameters {
    "integration.request.header.host" = "method.request.header.host"
    "integration.request.header.x_forwarded_port" = "'80'"
    "integration.request.header.x_forwarded_proto" = "'http'"
  }

  connection {timeout = "300"}
}

resource "aws_api_gateway_method_settings" "osm_borders_method_settings" {
  rest_api_id = "${aws_api_gateway_rest_api.osm_borders_api.id}"
  stage_name = "${aws_api_gateway_deployment.osm_borders_deployment.stage_name}"
  method_path = "${aws_api_gateway_resource.osm_borders_get_rest_api_resource.path_part}/${aws_api_gateway_method.osm_borders_api_method.http_method}"

  settings {
    logging_level = "INFO"
    throttling_rate_limit = "2"
    throttling_burst_limit = "10"
  }
}

resource "aws_api_gateway_account" "osm_api_gateway_account" {
  cloudwatch_role_arn = "${aws_iam_role.cloudwatch_log_role.arn}"
}

resource "aws_iam_role" "cloudwatch_log_role" {
  name = "cloudwatch_log"
  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "",
        "Effect": "Allow",
        "Principal": {
          "Service": "apigateway.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  }
EOF
}

resource "aws_sns_topic" "osm_api_osm_borders_topic" {
  name = "osm_api_osm_borders_topic"
}

resource "aws_sns_topic_subscription" "osm_api_osm_borders_subscription" {
  endpoint = "${aws_lambda_function.osm_borders_lambda_fetcher.arn}"
  protocol = "lambda"
  topic_arn = "${aws_sns_topic.osm_api_osm_borders_topic.arn}"
}

resource "aws_s3_bucket_object" "osm_borders_package_fetcher" {
  bucket = "${aws_s3_bucket.lambda_repo.id}"
  key = "osm_borders_package_fetcher"
  source = "../package_fetcher.zip"
  etag = "${md5(file("../package_fetcher.zip"))}"
  depends_on = ["null_resource.build_lambda"]
}

resource "aws_lambda_function" "osm_borders_lambda_fetcher" {
  s3_bucket = "${aws_s3_bucket_object.osm_borders_package_fetcher.bucket}"
  s3_key = "${aws_s3_bucket_object.osm_borders_package_fetcher.key}"
  function_name     = "osm-borders-fetcher"
  role              = "${aws_iam_role.osm_apps_lambda_iam.arn}"

  handler           = "fetcher.app"
  source_code_hash  = "${base64sha256(file("../package_fetcher.zip"))}"
  memory_size		= 1204
  runtime           = "python3.6"
  timeout = 300

  environment {
    variables {
      USE_AWS = "true"
      OSM_BORDERS_CACHE_TABLE = "${module.osm_prg_gminy_cache.name}"
    }
  }
  depends_on = ["null_resource.build_lambda"]
}


resource "aws_iam_role_policy_attachment" "cloudwatch_log" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
  role = "${aws_iam_role.cloudwatch_log_role.name}"
}

resource "aws_api_gateway_deployment" "osm_borders_deployment" {
  rest_api_id = "${aws_api_gateway_integration.osm_borders_api_integration.rest_api_id}"
  stage_name = "api"
  stage_description = "${timestamp()}" // workaround: terraform issue #6613?
}

output "url" {
  value = "${aws_api_gateway_deployment.osm_borders_deployment.invoke_url}/osm-borders/list/"
}

output "test_cal" {
  value = "${aws_api_gateway_deployment.osm_borders_deployment.invoke_url}/osm-borders/get/all/2807032.osm"
}

module "osm_prg_gminy_dict" {
  source = "./dynamo_cache"
  name = "osm_prg_gminy_v1"
}

module "osm_prg_gminy_cache" {
  source = "./output_cache"
  name = "osm_prg_gminy_cache_v1"
}

module "osm_prg_wojewodztwa_dict" {
  source = "./dynamo_cache"
  name = "osm_prg_wojewodztwa_v1"
}

module "osm_teryt_wmrodz_dict" {
  source = "./dynamo_cache"
  name = "osm_teryt_wmrodz_v1"
}

module "osm_teryt_simc_dict" {
  source = "./dynamo_cache"
  name = "osm_teryt_simc_v1"
}

module "osm_teryt_teryt_dict" {
  source = "./dynamo_cache"
  name = "osm_teryt_teryt_v1"
}

module "osm_teryt_ulic_dict" {
  source = "./dynamo_cache"
  name = "osm_teryt_ulic_v1"
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
