<?php

namespace Utopia\Storage\Device;

use Exception;
use Utopia\Storage\Device;

class MinIO extends Device
{

    /**
     * AWS Regions constants
     */
    const US_EAST_1 = 'us-east-1';
    const US_EAST_2 = 'us-east-2';
    const US_WEST_1 = 'us-west-1';
    const US_WEST_2 = 'us-west-2';
    const AF_SOUTH_1 = 'af-south-1';
    const AP_EAST_1 = 'ap-east-1';
    const AP_SOUTH_1 = 'ap-south-1';
    const AP_NORTHEAST_3 = 'ap-northeast-3';
    const AP_NORTHEAST_2 = 'ap-northeast-2';
    const AP_NORTHEAST_1 = 'ap-northeast-1';
    const AP_SOUTHEAST_1 = 'ap-southeast-1';
    const AP_SOUTHEAST_2 = 'ap-southeast-2';
    const CA_CENTRAL_1 = 'ca-central-1';
    const EU_CENTRAL_1 = 'eu-central-1';
    const EU_WEST_1 = 'eu-west-1';
    const EU_SOUTH_1 = 'eu-south-1';
    const EU_WEST_2 = 'eu-west-2';
    const EU_WEST_3 = 'eu-west-3';
    const EU_NORTH_1 = 'eu-north-1';
    const SA_EAST_1 = 'eu-north-1';
    const CN_NORTH_1 = 'cn-north-1';
    const ME_SOUTH_1 = 'me-south-1';
    const CN_NORTHWEST_1 = 'cn-northwest-1';
    const US_GOV_EAST_1 = 'us-gov-east-1';
    const US_GOV_WEST_1 = 'us-gov-west-1';

    public function __construct(string $root, string $accessKey, string $secretKey, string $protocol, string $host, string $bucket, string $region = self::EU_CENTRAL_1, string $acl = self::ACL_PRIVATE)
    {
        parent::__construct($root, $accessKey, $secretKey, $bucket, $region, $acl);
        
        $this->protocol = $protocol;
        $this->headers['host'] = $host;
    }

    /**
     * Get list of objects in the given path.
     *
     * @param string $path
     * 
     * @throws \Exception
     *
     * @return array
     */
    public function listObjects($prefix = '', $maxKeys = 1000, $continuationToken = '')
    {
        $uri = '/' . $this->getRoot();
        $this->headers['content-type'] = 'text/plain';
        $this->headers['content-md5'] = \base64_encode(md5('', true));

        $parameters = [
            'list-type' => 2,
            'prefix' => $prefix,
            'max-keys' => $maxKeys,
        ];
        if(!empty($continuationToken)) {
            $parameters['continuation-token'] = $continuationToken;
        }
        $response = parent::call(self::METHOD_GET, $uri, '', $parameters);
        return $response->body;
    }

    /**
     * Delete files in given path, path must be a directory. Return true on success and false on failure.
     *
     * @param string $path
     * 
     * @throws \Exception
     *
     * @return bool
     */
    public function deletePath(string $path): bool
    {
        $uri = '/' . $this->getRoot();
        $continuationToken = '';
        do {
            $objects = $this->listObjects($path, continuationToken: $continuationToken);
            $count = (int) ($objects['KeyCount'] ?? 1);
            if($count < 1) {
                break;
            }
            $continuationToken = $objects['NextContinuationToken'] ?? '';
            $body = '<Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">';
            
            if($count > 1) {
                foreach ($objects['Contents'] as $object) {
                    $body .= "<Object><Key>{$object['Key']}</Key></Object>";
                }
            } else {
                $body .= "<Object><Key>{$objects['Contents']['Key']}</Key></Object>"; 
            }
            $body .= '<Quiet>true</Quiet>';
            $body .= '</Delete>';
            $this->amzHeaders['x-amz-content-sha256'] = \hash('sha256', $body);
            $this->headers['content-md5'] = \base64_encode(md5($body, true));
            parent::call(self::METHOD_POST, $uri, $body, ['delete'=>'']);
        } while(!empty($continuationToken));

        return true;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return 'MinIO Object Storage';
    }

    /**
     * @return string
     */
    public function getDescription(): string
    {
        return 'MinIO Object Storage';
    }
}