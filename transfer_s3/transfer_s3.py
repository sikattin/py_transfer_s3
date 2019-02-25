#!/usr/bin/python3
#-------------------------------------------------------------------------------
# Name:        transfer_s3.py
# Purpose:     Transfer file/dir to S3 bucket.
#
# Author:      shikano.takeki
#
# Created:     21/02/2019
# Copyright:   (c) shikano.takeki 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
import sys
import os.path
import tarfile
import datetime
import configparser
from socket import gethostname
from s3_client.s3_client import S3Uploader
from botocore.exceptions import BotoCoreError, ClientError
from mylogger.factory import StdoutLoggerFactory, \
                             FileLoggerFactory, \
                             RotationLoggerFactory
from my_utils import my_utils

LOGGER_HANDLER = 'rotation'
LOGLEVEL = 20
ARCHIVE_MODE = "w:gz"

cfg_base = {
    "GENERAL": {
        "region": "ap-northeast-1"
    },
    "LOG": {
        "log_path": "/var/log/s3transfer.log",
        "log_rolloversize": 104857600
    },
    "CREDENTIAL": {
        "cred_section_name": "default",
    }
}


class TransferS3Base(object):
    """Base Class of transfer the specified file/dir to S3 bucket"""

    def __init__(self, bucket: str,
                 aws_cred_section=None,
                 aws_accesskey=None,
                 aws_secretkey=None,
                 aws_region=None,
                 logger=None,
                 handler=None,
                 is_accesskey_auth=False,
                 is_remove=True):
        """[summary]
        
        Args:
            bucket (str): [description]
            aws_cred_section ([type], optional): Defaults to None. [description]
            logger ([type], optional): Defaults to None. [description]
            handler ([type], optional): Defaults to None. [description]
            aws_accesskey ([type], optional): Defaults to None. [description]
            aws_secretkey ([type], optional): Defaults to None. [description]
            aws_region ([type], optional): Defaults to None. [description]
            is_accesskey_auth (bool, optional): Defaults to False. [description]
            is_remove (bool, optional): Defaults to True. [description]
        """
        if aws_region is None:
            aws_region = cfg_base['GENERAL']['region']
        if aws_cred_section is None:
            aws_cred_section = cfg_base['CREDENTIAL']['cred_section_name']
        self.bucket = bucket
        self.region = aws_region
        self.log_path = cfg_base['LOG']['log_path']
        self.log_rolloversize = cfg_base['LOG']['log_rolloversize']
        self.cred_section_name = aws_cred_section
        self.__aws_access = aws_accesskey
        self.__aws_secret = aws_secretkey
        self.is_accesskey_auth = is_accesskey_auth
        if self.__aws_access is not None and self.__aws_secret is not None:
            self.is_accesskey_auth = True
        self.is_remove = is_remove
        self._s3_session_args = {
            "aws_cred_secname": self.cred_section_name,
            "aws_region": self.region
        }
        self._s3_session_args_usekey = {
            "aws_accesskey": self.__aws_access,
            "aws_secretkey": self.__aws_secret,
            "aws_region": self.region
        }
        self.handler = handler
        self.loglevel = 20
        self._logger = logger

        if self.handler is None:
            self.handler = LOGGER_HANDLER
        if self._logger is None:
            self._logger = self._init_logger(self.log_path, self.handler)

        self._client = self._init_s3client(self.bucket)

    def _init_logger(self, logpath: str, handler: str):
        """Private only

        Initialize logger object
        """
        if os.path.isdir(os.path.split(logpath)[0]):
            os.makedirs(os.path.split(logpath)[0], exist_ok=True)
            if handler == 'file':
                flogger_fac = FileLoggerFactory(logger_name=__name__,
                                                loglevel=self.loglevel)
                logger = flogger_fac.create(file=logpath)
            elif handler == 'console':
                stdlogger_fac = StdoutLoggerFactory(logger_name=__name__,
                                                    loglevel=self.loglevel)
                logger = stdlogger_fac.create()
            elif handler == 'rotation':
                rlogger_fac = RotationLoggerFactory(logger_name=__name__,
                                                    loglevel=self.loglevel)
                logger = rlogger_fac.create(file=logpath,
                                                max_bytes=self.log_rolloversize,
                                                bcount=10)
            else:
                sys.stderr.write("an invalid value of logger handler " \
                                 "was thrown '{}' ." \
                                 " a valid values are console, file, rotation".format(self.handler))
                sys.stderr.flush()
                raise ValueError('An invalid parameter passed to {0}'.format(__name__))
        return logger

    def _init_config(self, conf_path: str):
        """Private only

        Initialize config
        """
        config = configparser.ConfigParser()
        config.read(conf_path)
        for k, v in config.items():
            if k == 'DEFAULT':
                continue
            for key, value in v.items():
                try:
                    cfg_base[k][key] = value
                except KeyError as key_e:
                    self._logger.error(str(key_e))
                    self._logger.error("invalid option set in config file.")
                    raise key_e
    
    def _init_s3client(self, bucket: str):
        """Private only
        
        Args:
            bucket (str): [description]
        """
        self._logger.info('Initialize S3 Client.')
        try:
            if self.is_accesskey_auth:
                client = S3Uploader(bucket, **self._s3_session_args_usekey)
            else:
                client = S3Uploader(bucket, **self._s3_session_args)
        except (BotoCoreError, ClientError) as e:
            self._logger.exception('raised unexpected error while initializing s3 uploader client.')
            self._logger.error(str(e))
        else:
            self._logger.info('Succeeded to initialize S3 Client object.')
            return client

    def compress_srcfile(self, src_path: str, archive_name=None):
        """[summary]
        
        Args:
            path (str): [description]
            archive_name ([type], optional): Defaults to None. [description]
        
        Raises:
            notfound_e: [description]
            tar_e: [description]
        """
        if archive_name is None:
            archive_name = r"{0}.tar.gz".format(src_path)
        try:
            self._logger.info('start to creates archive file {0}'.format(archive_name))
            with tarfile.open(archive_name, ARCHIVE_MODE) as tar:
                tar.add(src_path)
        except FileNotFoundError as notfound_e:
            self._logger.error('{0} not found.'.format(src_path))
            raise notfound_e
        except tarfile.TarError as tar_e:
            self._logger.error('raised unexpected error {0}'.format(tar_e))
            raise tar_e
        else:
            self._logger.info('created archive file {0}'.format(archive_name))
            return archive_name

    def upload(self, src_path: str, key_name=None):
        """Upload the specified file/dir to S3 bucket
        
        Args:
            src_path (str): [description]
            key_name ([type], optional): Defaults to None. [description]
        """
        if key_name is None:
            key_name = os.path.split(src_path)[1]
        self._logger.info('Start uploading {0} to amazon s3. ' \
            'uploading status is logging to /var/log/S3Operation.log'
            .format(src_path))
        try:
            self._client.upload(src_path, key_name=key_name)
        except (BotoCoreError, ClientError) as e:
            self._logger.error('raised unexpected error while uploading.')
            self._logger.exception(str(e))
            raise e
        except Exception as e:
            self._logger.error('raised unexpected error while uploading')
            self._logger.exception(str(e))
            raise e
        else:
            self._logger.info("complete uploading {0} to {1}".format(src_path, self.bucket))
        finally:
            if self.is_remove:
                os.remove(src_path)
                self._logger.info("Removed src file {0}".format(src_path))


class TransferS3Notification(TransferS3Base):
    """Transfer S3 plus mail notification
    
    Args:
        TransferS3Base ([type]): [description]
    """
    
    def __init__(self,
                 bucket: str,
                 smtp_server: str,
                 from_addr: str,
                 to_addr: str,
                 cc_addr: str,
                 aws_cred_section=None,
                 logger=None,
                 handler=None,
                 aws_accesskey=None,
                 aws_secretkey=None,
                 ses_accesskey=None,
                 ses_secretkey=None,
                 aws_region=None,
                 is_accesskey_auth=False,
                 is_ses_auth=False,
                 is_remove=False):
        """[summary]
        
        Args:
            bucket (str): [description]
            smtp_server (str): [description]
            from_addr (str): [description]
            to_addr (str): [description]
            cc_addr (str): [description]
            aws_cred_section ([type], optional): Defaults to None. [description]
            logger ([type], optional): Defaults to None. [description]
            handler ([type], optional): Defaults to None. [description]
            aws_accesskey ([type], optional): Defaults to None. [description]
            aws_secretkey ([type], optional): Defaults to None. [description]
            ses_accesskey ([type], optional): Defaults to None. [description]
            ses_secretkey ([type], optional): Defaults to None. [description]
            aws_region ([type], optional): Defaults to None. [description]
            is_accesskey_auth (bool, optional): Defaults to False. [description]
            is_ses_auth (bool, optional): Defaults to False. [description]
            is_remove (bool, optional): Defaults to False. [description]
        """
        super(TransferS3Notification, self).__init__(bucket,
                                                     aws_cred_section,
                                                     logger,
                                                     handler,
                                                     aws_accesskey,
                                                     aws_secretkey,
                                                     aws_region,
                                                     is_accesskey_auth,
                                                     is_remove,
        )
        self.mail_settings = {
            "smtp_server": smtp_server,
            "from_addr": from_addr,
            "to_addr": to_addr,
            "cc_addr": cc_addr
        }
        self.__ses_accesskey = ses_accesskey
        self.__ses_secretkey = ses_secretkey
        self.is_ses_auth = is_ses_auth
        if self.__ses_accesskey is not None and self.__ses_secretkey is not None:
            self.is_ses_auth = True
        self._now = datetime.datetime.now().strftime("%Y/%m/%d")

        self._mail = my_utils.MailUtil(**self.mail_settings,
                                       ses_accesskey=self.__ses_accesskey,
                                       ses_secretkey=self.__ses_secretkey,
                                       is_sesauth=self.is_ses_auth
        )
    
    def _init_s3client(self, bucket: str):
        """[summary]
        
        Args:
            bucket (str): [description]
        """
        self._logger.info('Initialize S3 Client.')
        try:
            if self.is_accesskey_auth:
                client = S3Uploader(bucket, **self._s3_session_args_usekey)
            else:
                client = S3Uploader(bucket, **self._s3_session_args)
        except (BotoCoreError, ClientError) as e:
            self._logger.exception('raised unexpected error while initializing s3 uploader client.')
            self._logger.error(str(e))
            self._mail.send_mail("[FAILED][{0}] {1} Transfer S3 - can't initialize S3 Client"
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(e)))
        else:
            self._logger.info('Succeeded to initialize S3 Client object.')
            return client

    def compress_srcfile(self, src_path: str, archive_name=None):
        """[summary]
        
        Args:
            path (str): [description]
            archive_name ([type], optional): Defaults to None. [description]
        
        Raises:
            notfound_e: [description]
            tar_e: [description]
        """
        if archive_name is None:
            archive_name = r"{0}.tar.gz".format(src_path)
        try:
            self._logger.info('start to creates archive file {0}'.format(archive_name))
            with tarfile.open(archive_name, ARCHIVE_MODE) as tar:
                tar.add(src_path)
        except FileNotFoundError as notfound_e:
            self._logger.error('{0} not found.'.format(src_path))
            self._mail.send_mail("[FAILED][{0}] {1} Transfer S3 - file not found"
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(notfound_e)))
            raise notfound_e
        except tarfile.TarError as tar_e:
            self._logger.error('raised unexpected error {0}'.format(tar_e))
            self._mail.send_mail("[FAILED][{0}] {1} Transfer S3 - failed compress to tar"
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(tar_e)))
            raise tar_e
        else:
            self._logger.info('created archive file {0}'.format(archive_name))
            return archive_name

    def upload(self, src_path: str, key_name=None):
        """Upload the specified file/dir to S3 bucket
        
        Args:
            src_path (str): [description]
            key_name ([type], optional): Defaults to None. [description]
        """
        if key_name is None:
            key_name = os.path.split(src_path)[1]
        self._logger.info('Start uploading {0} to amazon s3. ' \
            'uploading status is logging to /var/log/S3Operation.log'
            .format(src_path))
        try:
            self._client.upload(src_path, key_name=key_name)
        except (BotoCoreError, ClientError) as e:
            self._logger.error('raised unexpected error while uploading.')
            self._logger.exception(str(e))
            self._mail.send_mail("[FAILED][{0}] {1} Transfer S3 - failed uploading to S3"
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(e)))
            raise e
        except Exception as e:
            self._logger.error('raised unexpected error while uploading')
            self._logger.exception(str(e))
            self._mail.send_mail("[FAILED][{0}] {1} Transfer S3 - failed uploading to S3"
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(e)))
            raise e
        else:
            self._logger.info("complete uploading {0} to {1}".format(src_path, self.bucket))
            self._mail.send_mail("[SUCCESS][{0}] {1} Transfer S3 - "
                                 .format(gethostname(), self._now),
                                 "Transfer S3 failed.\nReason: {0}".format(str(e)))
        finally:
            if self.is_remove:
                os.remove(src_path)
                self._logger.info("Removed src file {0}".format(src_path))
