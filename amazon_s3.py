""" 
*********************************************************************** 
MIT License
Copyright (c) 2020 noamanfaisal
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
************************************************************************
"""

import pickle
import time
import uuid

import boto3
from botocore.exceptions import (ClientError, ConnectionClosedError,
                                 ConnectionError, ConnectTimeoutError,
                                 CredentialRetrievalError, DataNotFoundError,
                                 HTTPClientError, IncompleteReadError,
                                 NoCredentialsError, ReadTimeoutError)

from .s3_exceptions.s3_exceptions import S3BotoException, S3UnknownException


class AmazonS3:
    """
    it will save all S3 Amazon
    """

    def __init__(self, bucket_name, string_stamp):
        """
        docstring here
            :param self: 
            :param bucket_name: 
            :param string_stamp: 
        """
        self.__bucket_name = bucket_name
        self.__string_stamp = string_stamp
        self.__s3 = boto3.client('s3')

    def put_object(self, object_to_put):
        """
        put Python object to S3 using Picke
            :param self: 
            :param object_to_put: the object needed to put on S3
        """
        serializedListObject = pickle.dumps(object_to_put)
        try:
            key_name = self.__get_key_name()
            c = self.__s3.put_object(Bucket=self.__bucket_name, Key=key_name,
                                Body=serializedListObject)
            return key_name
        
        except ClientError as e:
            raise S3BotoException(e)

        except ConnectionClosedError as e:
            raise S3BotoException(e)

        except ConnectTimeoutError as e:
            raise S3BotoException(e)

        except ConnectionError as e:
            raise S3BotoException(e)

        except CredentialRetrievalError as e:
            raise S3BotoException(e)

        except HTTPClientError as e:
            raise S3BotoException(e)

        except Exception as e:
            raise S3UnknownException(e)

    def get_object(self, key_name):
        """
        docstring here
            :param self: 
            :param key_name: get object  from S3 and return it
        """
        try:
            pickle_object = self.__s3.get_object(Bucket=self.__bucket_name, Key=key_name)
            if pickle_object:
                if 'Body' in pickle_object.keys():
                    de_serialized_object = pickle.loads(pickle_object['Body'].read())
                    return de_serialized_object
            else:
                # raise Exception
                raise S3UnknownException('Could not get object from s3')
        
        except ClientError as e:
            raise S3BotoException(e)

        except ConnectionClosedError as e:
            raise S3BotoException(e)

        except ConnectTimeoutError as e:
            raise S3BotoException(e)

        except ConnectionError as e:
            raise S3BotoException(e)

        except CredentialRetrievalError as e:
            raise S3BotoException(e)

        except HTTPClientError as e:
            raise S3BotoException(e)

        except ReadTimeoutError as e:
            raise S3BotoException(e)

        except IncompleteReadError as e:
            raise S3BotoException(e)

        except Exception as e:
            raise S3UnknownException(e)
    
    def __get_current_unixtime_stamp(self):
        """
        get current unix time stamp
            :param self: 
        """
        return str(int(time.time()))


    def __get_guid(self):
        """
        get guid
            :param self: 
        """
        return str(uuid.uuid4())


    def __get_key_name(self):
        """
        get key name of S3
            :param self: 
        """
        return ''.join([self.__string_stamp, '_',self.__get_current_unixtime_stamp(), '_', self.__get_guid()])


