--
-- Create model Action
--
CREATE TABLE `seaflow_action` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `name` varchar(64) NOT NULL UNIQUE, `title` varchar(64) NOT NULL, `type` varchar(32) NOT NULL, `func` varchar(128) NULL UNIQUE, `input_def` json NOT NULL, `output_def` json NOT NULL, `create_time` datetime(6) NOT NULL, `update_time` datetime(6) NOT NULL);
--
-- Create model Dag
--
CREATE TABLE `seaflow_dag` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `name` varchar(64) NOT NULL, `title` varchar(64) NOT NULL, `version` integer UNSIGNED NULL CHECK (`version` >= 0), `latest` bool NULL, `input_adapter` json NOT NULL, `output_adapter` json NOT NULL, `fissionable` bool NOT NULL, `fission_config` json NOT NULL, `iterable` bool NOT NULL, `iter_config` json NOT NULL, `loopable` bool NOT NULL, `loop_config` json NOT NULL, `max_retries` integer NOT NULL, `retry_countdown` integer NOT NULL, `timeout` integer NULL, `create_time` datetime(6) NOT NULL, `update_time` datetime(6) NOT NULL, `parent_id` integer NULL);
CREATE TABLE `seaflow_dag_to_dag` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `from_dag_id` integer NOT NULL, `to_dag_id` integer NOT NULL);
--
-- Create model Log
--
CREATE TABLE `seaflow_log` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `ref_type` varchar(16) NOT NULL, `ref_id` integer NOT NULL, `ts` double precision NOT NULL, `content` longtext NOT NULL);
--
-- Create model Node
--
CREATE TABLE `seaflow_node` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `name` varchar(64) NOT NULL, `title` varchar(64) NOT NULL, `input_adapter` json NOT NULL, `output_adapter` json NOT NULL, `action_type` varchar(32) NOT NULL, `fissionable` bool NOT NULL, `fission_config` json NOT NULL, `iterable` bool NOT NULL, `iter_config` json NOT NULL, `loopable` bool NOT NULL, `loop_config` json NOT NULL, `max_retries` integer NOT NULL, `retry_countdown` integer NOT NULL, `timeout` integer NULL, `create_time` datetime(6) NOT NULL, `update_time` datetime(6) NOT NULL, `action_id` integer NOT NULL, `dag_id` integer NOT NULL, `root_dag_id` integer NOT NULL);
CREATE TABLE `seaflow_dag_to_node` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `node_id` integer NOT NULL, `dag_id` integer NOT NULL);
CREATE TABLE `seaflow_node_to_node` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `from_node_id` integer NOT NULL, `to_node_id` integer NOT NULL);
--
-- Create model Step
--
CREATE TABLE `seaflow_step` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `identifier` varchar(64) NULL, `name` varchar(64) NOT NULL, `title` varchar(64) NOT NULL, `state` varchar(64) NOT NULL, `config` json NOT NULL, `retries` integer NOT NULL, `fission_count` integer NOT NULL, `fission_index` integer NOT NULL, `iter_index` integer NOT NULL, `iter_end` bool NULL, `loop_index` integer NOT NULL, `input` json NOT NULL, `output` json NOT NULL, `extra` json NOT NULL, `error` longtext NOT NULL, `logs` json NOT NULL, `logfile` varchar(256) NOT NULL, `log_refetch_time` datetime(6) NULL, `start_time` datetime(6) NULL, `end_time` datetime(6) NULL, `duration` double precision NULL, `create_time` datetime(6) NOT NULL, `update_time` datetime(6) NOT NULL, `node_id` integer NOT NULL);
CREATE TABLE `seaflow_step_to_step` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `from_step_id` integer NOT NULL, `to_step_id` integer NOT NULL);
--
-- Create model Task
--
CREATE TABLE `seaflow_task` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `name` varchar(64) NOT NULL, `title` varchar(64) NOT NULL, `state` varchar(64) NOT NULL, `config` json NOT NULL, `retries` integer NOT NULL, `fission_count` integer NOT NULL, `fission_index` integer NOT NULL, `iter_index` integer NOT NULL, `iter_end` bool NULL, `loop_index` integer NOT NULL, `input` json NOT NULL, `context` json NOT NULL, `output` json NOT NULL, `extra` json NOT NULL, `error` longtext NOT NULL, `logs` json NOT NULL, `logfile` varchar(256) NOT NULL, `log_refetch_time` datetime(6) NULL, `start_time` datetime(6) NULL, `end_time` datetime(6) NULL, `duration` double precision NULL, `create_time` datetime(6) NOT NULL, `update_time` datetime(6) NOT NULL, `dag_id` integer NOT NULL, `parent_id` integer NULL, `root_id` integer NULL);
CREATE TABLE `seaflow_step_to_task` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `task_id` integer NOT NULL, `step_id` integer NOT NULL);
CREATE TABLE `seaflow_task_to_task` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `from_task_id` integer NOT NULL, `to_task_id` integer NOT NULL);
--
-- Add field previous_tasks to step
--
CREATE TABLE `seaflow_task_to_step` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `step_id` integer NOT NULL, `task_id` integer NOT NULL);
--
-- Add field root to step
--
ALTER TABLE `seaflow_step` ADD COLUMN `root_id` integer NOT NULL;
--
-- Add field task to step
--
ALTER TABLE `seaflow_step` ADD COLUMN `task_id` integer NOT NULL;
--
-- Add field previous_nodes to dag
--
CREATE TABLE `seaflow_node_to_dag` (`id` integer AUTO_INCREMENT NOT NULL PRIMARY KEY, `dag_id` integer NOT NULL, `node_id` integer NOT NULL);
--
-- Add field root to dag
--
ALTER TABLE `seaflow_dag` ADD COLUMN `root_id` integer NULL;
--
-- Alter unique_together for step (1 constraint(s))
--
ALTER TABLE `seaflow_step` ADD CONSTRAINT `seaflow_step_task_id_node_id_fission__69fab8ea_uniq` UNIQUE (`task_id`, `node_id`, `fission_index`, `iter_index`);
CREATE INDEX `seaflow_dag_name_2b866319` ON `seaflow_dag` (`name`);
CREATE INDEX `seaflow_dag_version_e31da05d` ON `seaflow_dag` (`version`);
CREATE INDEX `seaflow_dag_parent_id_940db85b` ON `seaflow_dag` (`parent_id`);
ALTER TABLE `seaflow_dag_to_dag` ADD CONSTRAINT `seaflow_dag_to_dag_from_dag_id_to_dag_id_08f0999d_uniq` UNIQUE (`from_dag_id`, `to_dag_id`);
CREATE INDEX `seaflow_dag_to_dag_from_dag_id_2dc4f9c9` ON `seaflow_dag_to_dag` (`from_dag_id`);
CREATE INDEX `seaflow_dag_to_dag_to_dag_id_dd5327fc` ON `seaflow_dag_to_dag` (`to_dag_id`);
CREATE INDEX `seaflow_log_ref_id_13884e51` ON `seaflow_log` (`ref_id`);
CREATE INDEX `seaflow_node_action_id_f4627bb5` ON `seaflow_node` (`action_id`);
CREATE INDEX `seaflow_node_dag_id_878599f7` ON `seaflow_node` (`dag_id`);
CREATE INDEX `seaflow_node_root_dag_id_4ede0c4e` ON `seaflow_node` (`root_dag_id`);
ALTER TABLE `seaflow_dag_to_node` ADD CONSTRAINT `seaflow_dag_to_node_node_id_dag_id_25509f32_uniq` UNIQUE (`node_id`, `dag_id`);
CREATE INDEX `seaflow_dag_to_node_node_id_f3120560` ON `seaflow_dag_to_node` (`node_id`);
CREATE INDEX `seaflow_dag_to_node_dag_id_84f8c931` ON `seaflow_dag_to_node` (`dag_id`);
ALTER TABLE `seaflow_node_to_node` ADD CONSTRAINT `seaflow_node_to_node_from_node_id_to_node_id_ee4c900d_uniq` UNIQUE (`from_node_id`, `to_node_id`);
CREATE INDEX `seaflow_node_to_node_from_node_id_d668f94b` ON `seaflow_node_to_node` (`from_node_id`);
CREATE INDEX `seaflow_node_to_node_to_node_id_8f15f4bf` ON `seaflow_node_to_node` (`to_node_id`);
CREATE INDEX `seaflow_step_identifier_fb59d718` ON `seaflow_step` (`identifier`);
CREATE INDEX `seaflow_step_fission_index_ff133271` ON `seaflow_step` (`fission_index`);
CREATE INDEX `seaflow_step_iter_index_ae930755` ON `seaflow_step` (`iter_index`);
CREATE INDEX `seaflow_step_log_refetch_time_3ca2bbe6` ON `seaflow_step` (`log_refetch_time`);
CREATE INDEX `seaflow_step_node_id_fddc819c` ON `seaflow_step` (`node_id`);
ALTER TABLE `seaflow_step_to_step` ADD CONSTRAINT `seaflow_step_to_step_from_step_id_to_step_id_a93a5f93_uniq` UNIQUE (`from_step_id`, `to_step_id`);
CREATE INDEX `seaflow_step_to_step_from_step_id_eb454a18` ON `seaflow_step_to_step` (`from_step_id`);
CREATE INDEX `seaflow_step_to_step_to_step_id_db5c6dd6` ON `seaflow_step_to_step` (`to_step_id`);
ALTER TABLE `seaflow_task` ADD CONSTRAINT `seaflow_task_parent_id_dag_id_fission_118db3e1_uniq` UNIQUE (`parent_id`, `dag_id`, `fission_index`, `iter_index`);
CREATE INDEX `seaflow_task_fission_index_12d3032d` ON `seaflow_task` (`fission_index`);
CREATE INDEX `seaflow_task_iter_index_8b967e19` ON `seaflow_task` (`iter_index`);
CREATE INDEX `seaflow_task_log_refetch_time_f960a8a7` ON `seaflow_task` (`log_refetch_time`);
CREATE INDEX `seaflow_task_dag_id_65a80208` ON `seaflow_task` (`dag_id`);
CREATE INDEX `seaflow_task_parent_id_7f3f7e1f` ON `seaflow_task` (`parent_id`);
CREATE INDEX `seaflow_task_root_id_5807222a` ON `seaflow_task` (`root_id`);
ALTER TABLE `seaflow_step_to_task` ADD CONSTRAINT `seaflow_step_to_task_task_id_step_id_b6f6184b_uniq` UNIQUE (`task_id`, `step_id`);
CREATE INDEX `seaflow_step_to_task_task_id_5e9c53b4` ON `seaflow_step_to_task` (`task_id`);
CREATE INDEX `seaflow_step_to_task_step_id_51a3047f` ON `seaflow_step_to_task` (`step_id`);
ALTER TABLE `seaflow_task_to_task` ADD CONSTRAINT `seaflow_task_to_task_from_task_id_to_task_id_8b494042_uniq` UNIQUE (`from_task_id`, `to_task_id`);
CREATE INDEX `seaflow_task_to_task_from_task_id_be15544d` ON `seaflow_task_to_task` (`from_task_id`);
CREATE INDEX `seaflow_task_to_task_to_task_id_a904e852` ON `seaflow_task_to_task` (`to_task_id`);
ALTER TABLE `seaflow_task_to_step` ADD CONSTRAINT `seaflow_task_to_step_step_id_task_id_79c50d17_uniq` UNIQUE (`step_id`, `task_id`);
CREATE INDEX `seaflow_task_to_step_step_id_ed36fb10` ON `seaflow_task_to_step` (`step_id`);
CREATE INDEX `seaflow_task_to_step_task_id_65f74ecf` ON `seaflow_task_to_step` (`task_id`);
CREATE INDEX `seaflow_step_root_id_3b42bfea` ON `seaflow_step` (`root_id`);
CREATE INDEX `seaflow_step_task_id_cf190d59` ON `seaflow_step` (`task_id`);
ALTER TABLE `seaflow_node_to_dag` ADD CONSTRAINT `seaflow_node_to_dag_dag_id_node_id_136c8290_uniq` UNIQUE (`dag_id`, `node_id`);
CREATE INDEX `seaflow_node_to_dag_dag_id_9e4a71fc` ON `seaflow_node_to_dag` (`dag_id`);
CREATE INDEX `seaflow_node_to_dag_node_id_d9cf28c1` ON `seaflow_node_to_dag` (`node_id`);
CREATE INDEX `seaflow_dag_root_id_c5cc1ad9` ON `seaflow_dag` (`root_id`);
