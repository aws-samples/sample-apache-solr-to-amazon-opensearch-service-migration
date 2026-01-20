import {Construct} from "constructs";
import {
    BastionHostLinux,
    BlockDeviceVolume,
    InstanceClass,
    InstanceSize,
    InstanceType,
    MachineImage,
    SecurityGroup,
    Vpc,
    CloudFormationInit,
    InitFile,
    InitCommand,
    InitPackage
} from "aws-cdk-lib/aws-ec2";
import {Effect, PolicyStatement} from "aws-cdk-lib/aws-iam";
import {Stack} from "aws-cdk-lib";
import * as cdk from "aws-cdk-lib";

export interface Ec2WorkBenchProps {
    readonly vpc: Vpc;
    readonly migrationBucketName: string;
    readonly domainName: string;
    readonly opensearchEndpoint: string;
    readonly indexName: string;
    readonly opensearchSecretName: string;
    readonly pipelineName: string;
}

export class Ec2workbench extends Construct {

    readonly host: BastionHostLinux;

    constructor(scope: Construct, id: string, props: Ec2WorkBenchProps) {
        super(scope, id);

        const project_name = "sample-apache-solr-to-amazon-opensearch-service-migration"
        const host = new BastionHostLinux(this, 'BastionHost', {
            vpc: props.vpc,
            instanceType: InstanceType.of(InstanceClass.T3, InstanceSize.MEDIUM),
            machineImage: MachineImage.latestAmazonLinux2023(),
            blockDevices: [{
                deviceName: '/dev/xvda',
                volume: BlockDeviceVolume.ebs(20, {
                    encrypted: true,
                }),
            }],
            securityGroup: new SecurityGroup(this, 'SecurityGroup', {
                vpc: props.vpc,
                allowAllOutbound: true,
            }),
            userDataCausesReplacement: false,
            init: CloudFormationInit.fromElements(
                // Install packages
                InitPackage.yum('python3'),
                InitPackage.yum('python3-pip'),
                InitPackage.yum('git'),
                
                InitCommand.shellCommand('cd /home/ec2-user'),
                InitCommand.shellCommand(`git clone https://github.com/aws-samples/${project_name}.git`),

                // Fix ownership and permissions
                InitCommand.shellCommand('chown -R ec2-user:ec2-user /home/ec2-user/'),
                InitCommand.shellCommand('chmod -R 755 /home/ec2-user/'),
                InitCommand.shellCommand(`sudo -u ec2-user bash -c "cd /home/ec2-user/${project_name} && pip3 install --user -r requirements.txt"`),
                
                // Create migrate.toml using fromString
                InitFile.fromString(`/home/ec2-user/${project_name}/migrate.toml`, `[solr]
host="http://your-solr-host.com"
port=8983
username="your-solr-username"
password="your-solr-password"
collection="your-collection-name"

[opensearch]

domain="${props.domainName}"
bucket="${props.migrationBucketName}"
host="${props.opensearchEndpoint}"
port=443
region="${Stack.of(this).region}"
use_aws_auth_sigv4=true
username="{{OPENSEARCH_USERNAME}}"
password="{{OPENSEARCH_PASSWORD}}"
index="${props.indexName}"
use_ssl=true
assert_hostname=false
verify_certs=true

[migration]
migrate_schema=true
create_package=true
expand_files_array=false
create_index=false
skip_text_analysis_failure=false

[data_migration]
migrate_data=true
s3_export_bucket="${props.migrationBucketName}"
s3_export_prefix="migration_data/"
region="${Stack.of(this).region}"
batch_size=1000
rows_per_page=500
max_rows=100000
`),
                // Set ownership
                InitCommand.shellCommand(`chown ec2-user:ec2-user /home/ec2-user/${project_name}/migrate.toml`)
            ),
            initOptions: {
                timeout: cdk.Duration.minutes(10),
                includeRole: true,
                printLog: true,
                embedFingerprint: false,
                ignoreFailures: true
            }
        });
        
        this.host = host;

        host.instance.addToRolePolicy(
            new PolicyStatement({
                sid: "S3Packages",
                actions: [
                    "es:ListPackagesForDomain",
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:HeadObject",
                    "es:AssociatePackage",
                    "es:DissociatePackage",
                    "es:*"
                ],
                resources: [
                    `arn:aws:s3:::${props.migrationBucketName}`,
                    `arn:aws:s3:::${props.migrationBucketName}/*`,
                    `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                    `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                ],
                effect: Effect.ALLOW
            })
        )

        host.instance.addToRolePolicy(
            new PolicyStatement({
                sid: "OSPackages",
                actions: [
                    "es:ListDomainsForPackage",
                    "es:CreatePackage",
                    "es:UpdatePackage",
                    "es:DescribePackages",
                    "es:GetPackageVersionHistory"
                ],
                resources: [
                    "*"
                ],
                effect: Effect.ALLOW
            })
        )

    }
}