 #task表，用来存储J2EE平台插入其中的任务的信息
 CREATE TABLE `task` (
 `task_id` int(11) NOT NULL AUTO_INCREMENT,
 `task_name` varchar(255) DEFAULT NULL,
 `create_time` datetime DEFAULT NULL,
 `start_time` datetime DEFAULT NULL,
 `finish_time` datetime DEFAULT NULL,
 `task_type` varchar(255) DEFAULT NULL,
 `task_status` varchar(255) DEFAULT NULL,
 `task_param` text,
 PRIMARY KEY (`task_id`)
 ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;