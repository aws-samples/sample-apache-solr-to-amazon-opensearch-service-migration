import {Construct} from "constructs";
import {Effect, Policy, PolicyStatement, Role} from "aws-cdk-lib/aws-iam";
import {CustomResource, Duration, Stack} from "aws-cdk-lib";
import {IVpc} from "aws-cdk-lib/aws-ec2";
import * as fs from "fs";
import {Code, Function, Runtime} from "aws-cdk-lib/aws-lambda";

export interface OpenSearchRoleMapperProps {
    readonly vpc: IVpc;
    readonly iamRoleArns?: string;
    readonly roleName: string;
    readonly domainEndpoint: string;
    readonly domainName: string;
    readonly secretName: string;
}

export class OpenSearchRoleMapper extends Construct {
    public readonly cr: CustomResource;
    constructor(scope: Construct, id: string, props: OpenSearchRoleMapperProps) {
        super(scope, id);

        const lambda = new Function(this, "CustomResourceFunction", {
            runtime: Runtime.PYTHON_3_9,
            handler: "index.handler",
            timeout: Duration.seconds(30),
            code: Code.fromInline(fs.readFileSync('lib/lambda/lambda_function.py', 'utf8')),
            vpc: props.vpc,
            vpcSubnets: {
                subnets: props.vpc.privateSubnets
            },
            environment: {
                "OS_SECRET_NAME": props.secretName,
            },
            initialPolicy:  [
                new PolicyStatement({
                    sid: "VPCAccess",
                    actions: [
                        "ec2:CreateNetworkInterface",
                        "ec2:DescribeNetworkInterfaces",
                        "ec2:DeleteNetworkInterface",
                        "ec2:DescribeVpcs",
                        "ec2:DescribeSubnets",
                        "ec2:DescribeSecurityGroups"
                    ],
                    effect: Effect.ALLOW,
                    resources: ["*"]
                }),
                new PolicyStatement({
                    sid: "OpensearchAccess",
                    actions: [
                        'es:ESHttpPut',
                        'es:ESHttpDelete',
                        'es:ESHttpGet'
                    ],
                    resources: [
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                    ],
                    effect: Effect.ALLOW
                }),
                new PolicyStatement({
                    sid: "SecretManagerAccess",
                    actions: [
                        'secretsmanager:GetSecretValue',
                        'secretsmanager:DescribeSecret',
                        'secretsmanager:ListSecretVersionIds'
                    ],
                    resources: [`arn:aws:secretsmanager:${Stack.of(this).region}:${Stack.of(this).account}:secret:${props.secretName}*`],
                    effect: Effect.ALLOW
                })
            ]
        })


        this.cr = new CustomResource(this,"CustomResource", {
            serviceToken: lambda.functionArn,
            properties: {
                DomainEndpoint: props.domainEndpoint,
                RoleName: props.roleName,
                IamRoleArns: props.iamRoleArns,
                Region: Stack.of(this).region
            },
            serviceTimeout: Duration.minutes(1)
        });


    }
}


