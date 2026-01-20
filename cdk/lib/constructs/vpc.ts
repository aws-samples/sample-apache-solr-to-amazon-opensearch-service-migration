import {Construct} from "constructs";
import {aws_ec2} from 'aws-cdk-lib';
import {IpAddresses} from "aws-cdk-lib/aws-ec2";

export interface VpcConstructProps {
    vpcCidr: string;
    vpcName?: string;
}

export class VpcConstruct extends Construct {
    public readonly vpc;

    constructor(scope: Construct, id: string, props: VpcConstructProps) {
        super(scope, id);

        this.vpc = new aws_ec2.Vpc(this, "VPC", {
            vpcName: props.vpcName || "solr2os-vpc",
            ipAddresses: IpAddresses.cidr(props.vpcCidr),
            maxAzs: 2,
            natGateways: 1,
            subnetConfiguration: [{
                name: 'public', subnetType: aws_ec2.SubnetType.PUBLIC
            }, {
                name: 'private', subnetType: aws_ec2.SubnetType.PRIVATE_WITH_EGRESS
            }]
        });
    }

}