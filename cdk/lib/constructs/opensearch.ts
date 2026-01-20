import {Construct} from "constructs";
import {Domain, EngineVersion} from "aws-cdk-lib/aws-opensearchservice";
import {ISubnet, IVpc, Peer, Port, SecurityGroup} from "aws-cdk-lib/aws-ec2";
import {AnyPrincipal, ArnPrincipal, Effect, PolicyStatement} from "aws-cdk-lib/aws-iam";
import {RemovalPolicy, Stack} from "aws-cdk-lib";
import {Secret} from "aws-cdk-lib/aws-secretsmanager";

interface OpensearchConstructProps {
    vpc: IVpc;
    subnets: ISubnet[];
    domainName?: string;
    instanceType?: string;
    instanceCount?: number;
    volumeSize?: number;
    version?: EngineVersion;
    pipelineRoleArn: string;
    ec2WorkBenchRoleArn?: string;

}

export class OpenSearchConstruct extends Construct {
    public readonly domain: Domain;
    public readonly secret: Secret;

    constructor(scope: Construct, id: string, props: OpensearchConstructProps) {
        super(scope, id);

        const defaultProps = {
            vpc: props.vpc,
            subnets: props.subnets,
            domainName: props.domainName || "solr2os-migration",
            instanceType: props.instanceType || "t3.small.search",
            instanceCount: props.instanceCount || 2,
            volumeSize: props.volumeSize || 30,
            version: props.version || EngineVersion.OPENSEARCH_2_19,
            pipelineRoleArn: props.pipelineRoleArn,
            ec2WorkBenchRoleArn: props.ec2WorkBenchRoleArn
        };

        const securityGroup = new SecurityGroup(this, "SecurityGroup", {
            vpc: defaultProps.vpc, description: 'Security group for OpenSearch domain', allowAllOutbound: true
        });
        securityGroup.addIngressRule(Peer.ipv4(props.vpc.vpcCidrBlock), Port.tcp(443))

        const secret = new Secret(this, 'Secret', {
            generateSecretString: {
                secretStringTemplate: JSON.stringify({
                    username: 'admin'
                }), generateStringKey: 'password',
                excludeCharacters: ' "\\\';:/&%`.',
            }, removalPolicy: RemovalPolicy.DESTROY
        })
        this.secret = secret;

        this.domain = new Domain(this, "Domain", {
            version: defaultProps.version,
            domainName: defaultProps.domainName,
            enableVersionUpgrade: true,
            enforceHttps: true,
            nodeToNodeEncryption: true,
            encryptionAtRest: {
                enabled: true
            },
            fineGrainedAccessControl: {
                masterUserName: "admin",
                masterUserPassword: Secret.fromSecretAttributes(this, 'OSSecret', secret).secretValueFromJson('password')
            },
            securityGroups: [securityGroup],
            capacity: {
                dataNodes: defaultProps.instanceCount,
                dataNodeInstanceType: defaultProps.instanceType,
                multiAzWithStandbyEnabled: false
            },
            zoneAwareness: {
                enabled: true, availabilityZoneCount: 2,
            },
            vpc: defaultProps.vpc,
            vpcSubnets: [{subnets: defaultProps.subnets}],
            removalPolicy: RemovalPolicy.DESTROY,
            accessPolicies: [
                new PolicyStatement({
                    sid: "PipelineAccess",
                    principals: [
                        new ArnPrincipal(props.pipelineRoleArn),
                        ...(props.ec2WorkBenchRoleArn ? [new ArnPrincipal(props.ec2WorkBenchRoleArn)] : [])
                    ],
                    actions: [
                        'es:ESHttp*',
                        'es:DescribeElasticsearchDomain',
                        'es:ListDomainNames',
                        'es:DescribeElasticsearchDomains'
                    ],
                    resources: [
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                    ],
                    effect: Effect.ALLOW
                }),
                new PolicyStatement({
                    sid: "DashboardAccess",
                    principals: [new AnyPrincipal(),],
                    actions: ['es:ESHttp*'],
                    resources: [
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}`,
                        `arn:aws:es:${Stack.of(this).region}:${Stack.of(this).account}:domain/${props.domainName}/*`
                    ],
                    effect: Effect.ALLOW
                })
            ]
        });
    }

}