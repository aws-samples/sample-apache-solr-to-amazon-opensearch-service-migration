import {Construct} from "constructs";
import {Effect, Policy, PolicyStatement, Role, ServicePrincipal} from "aws-cdk-lib/aws-iam";
import {Stack} from "aws-cdk-lib";
import {IVpc} from "aws-cdk-lib/aws-ec2";

export interface PipelineIamUserProps {
    readonly vpc: IVpc;
    readonly pipelineName?: string;
    readonly migrationBucketName: string;
    readonly domainName: string;
}

export class PipelineIamRole extends Construct {

    readonly pipelineRole: Role;

    constructor(scope: Construct, id: string, props: PipelineIamUserProps) {
        super(scope, id);

        const pipelineRole = new Role(this, 'Role', {
            assumedBy: new ServicePrincipal("osis-pipelines.amazonaws.com"),
        });
        this.pipelineRole = pipelineRole

        pipelineRole.attachInlinePolicy(new Policy(this, "PipelinePolicy", {
            statements: [
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
                        "es:ESHttp*",
                        "es:DescribeDomain",
                        "es:ListDomain",
                        "es:DescribeDomainConfig",
                        "es:GetCompatibleVersions"
                    ],
                    resources: [
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                    ],
                    effect: Effect.ALLOW
                }),
                new PolicyStatement({
                    sid: "OpensearchAccess",
                    actions: [
                        "es:ESHttp*",
                        "es:DescribeDomain",
                        "es:ListDomain",
                        "es:DescribeDomainConfig",
                        "es:GetCompatibleVersions"
                    ],
                    resources: [
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                    ],
                    effect: Effect.ALLOW
                }),
                new PolicyStatement({
                    sid: "S3Access",
                    actions: [
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                        "s3:PutObject"
                    ],
                    resources: [
                        `arn:aws:s3:::${props.migrationBucketName}`,
                        `arn:aws:s3:::${props.migrationBucketName}/migration_data/*`
                    ],
                    effect: Effect.ALLOW
                })
            ]
        }))
    }
}


