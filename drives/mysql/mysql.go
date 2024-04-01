package mysql

import (
	"fmt"
	"github.com/eopenio/itask/v3/message"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"time"
)

type Result struct {
	Id         int64     `json:"id,omitempty" gorm:"primaryKey"`
	TaskId     string    `json:"taskId,omitempty" gorm:"column:task_id;comment:主任务ID;type:varchar(50);size:50;uniqueIndex:idx_taskid"`
	TaskName   string    `json:"taskName,omitempty" gorm:"column:task_name;comment:任务名称;type:varchar(50);size:50;"`
	TaskDesc   string    `json:"taskDesc,omitempty" gorm:"column:task_desc;comment:任务描述;type:varchar(100);size:100;"`
	OpUser     string    `json:"opUser,omitempty" gorm:"column:op_user;comment:操作人员;type:varchar(50);size:50;"`
	OpArgs     string    `json:"opArgs,omitempty" gorm:"column:op_args;comment:任务参数;type:mediumtext;"`
	Total      int       `json:"total,omitempty" gorm:"column:total;comment:待执行任务总数;type:int;size:10;"`
	Step       int       `json:"step,omitempty" gorm:"column:step;comment:当前任务节点;type:int;size:10;"`
	RetryCount int       `json:"retryCount,omitempty" gorm:"column:retry_count;comment:任务重试次数;type:int;size:10;"`
	Status     string    `json:"status,omitempty" gorm:"column:status;comment:主任务状态;type:int;size:10;index:idx_status"`
	FuncReturn string    `json:"funcReturn,omitempty" gorm:"column:func_return;comment:任务返回内容;type:mediumtext;"`
	WorkFlow   string    `json:"workFlow,omitempty" gorm:"column:work_flow;comment:任务流状态;type:text"`
	ErrorMsg   string    `json:"errorMsg,omitempty" gorm:"column:error_msg;comment:错误信息;type:varchar(256);size:50;"`
	CreateAt   time.Time `json:"createAt,omitempty" gorm:"column:create_at;comment:创建时间;type:TIMESTAMP;default:CURRENT_TIMESTAMP;<-:CREATE;index:idx_createAt"`
	UpdateAt   time.Time `json:"updateAt,omitempty" gorm:"column:update_at;comment:更新时间;type:TIMESTAMP;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"`
}

func (Result) TableName() string {
	return "tb_idb_task_result"
}

type MsgResult struct {
	Id            string   `json:"id" gorm:"column:task_id;comment:任务ID;type:varchar(50);size:50;primaryKey"`
	Status        int      `json:"status" gorm:"column:status;comment:任务状态;type:int;size:10;index:idx_status"` // 0:sent , 1:first running , 2: waiting to retry , 3: running , 4: success , 5: Failure
	FuncReturn    []string `json:"func_return" gorm:"column:func_return;comment:任务返回内容;type:mediumtext;"`
	FuncReturnStr string
	RetryCount    int         `json:"retry_count" gorm:"column:retry_count;comment:任务重试次数;type:int;size:10;"`
	Workflow      [][2]string `json:"workflow" gorm:"column:work_flow;comment:任务流状态;type:text"` // [["workName","status"],] ;  status: waiting , running , success , failure , expired , abort
	WorkflowStr   string
	Err           string `json:"err" gorm:"column:error_msg;comment:错误信息;type:varchar(256);size:50;"`
}

type MsgResultTable struct {
	Id         int64     `json:"id,omitempty" gorm:"primaryKey"`
	TaskId     string    `json:"taskId,omitempty" gorm:"column:task_id;comment:主任务ID;type:varchar(50);size:50;uniqueIndex:idx_taskid"`
	Status     int       `json:"status" gorm:"column:status;comment:任务状态;type:int;size:10;index:idx_status"` // 0:sent , 1:first running , 2: waiting to retry , 3: running , 4: success , 5: Failure
	FuncReturn string    `json:"func_return" gorm:"column:func_return;comment:任务返回内容;type:mediumtext;"`
	RetryCount int       `json:"retry_count" gorm:"column:retry_count;comment:任务重试次数;type:int;size:10;"`
	Workflow   string    `json:"workflow" gorm:"column:work_flow;comment:任务流状态;type:text"` // [["workName","status"],] ;  status: waiting , running , success , failure , expired , abort
	Err        string    `json:"err" gorm:"column:error_msg;comment:错误信息;type:varchar(256);size:50;"`
	CreateAt   time.Time `json:"createAt,omitempty" gorm:"column:create_at;comment:创建时间;type:TIMESTAMP;default:CURRENT_TIMESTAMP;<-:CREATE;index:idx_createAt"`
	UpdateAt   time.Time `json:"updateAt,omitempty" gorm:"column:update_at;comment:更新时间;type:TIMESTAMP;default:CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"`
}

func (MsgResultTable) TableName() string {
	return "tb_message_result"
}

type Client struct {
	Config   mysql.Config
	idleConn int
	idleTime int
	maxConn  int
	maxTime  int
	mysql    *gorm.DB
}

func NewMySQLClient(host, port, user, password, dbname string,
	idleConn, maxConn, idleTime, maxTime int) *Client {

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", user, password, host, port, dbname)
	config := mysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         191,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}
	client := Client{
		Config:   config,
		idleConn: idleConn,
		maxConn:  maxConn,
		idleTime: idleTime,
		maxTime:  maxTime,
		mysql:    nil,
	}

	if err := client.Init(); err != nil {
		panic("Task: init mysql backend error: " + err.Error())
	}

	if err := client.AutoMigrate(); err != nil {
		panic("Task: init mysql backend error: " + err.Error())
	}

	return &client
}

func (c *Client) Exists(taskId string) (bool, error) {
	count := int64(0)
	err := c.mysql.Model(&Result{}).Where("task_id = ?", taskId).Count(&count).Error
	if count == 0 {
		return true, err
	}
	return false, err
}

func (c *Client) Init() error {
	db, err := gorm.Open(mysql.New(c.Config))
	if err != nil {
		return err
	}
	c.mysql = db
	sqlDB, _ := c.mysql.DB()
	sqlDB.SetMaxIdleConns(c.idleConn)
	sqlDB.SetMaxOpenConns(c.maxConn)
	sqlDB.SetConnMaxIdleTime(30 * time.Minute)
	sqlDB.SetConnMaxLifetime(60 * time.Minute)
	if err = sqlDB.Ping(); err != nil {
		return err
	}
	return nil
}

func (c *Client) AutoMigrate() error {
	//if err := c.mysql.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&Result{}); err != nil {
	//	return err
	//}
	if err := c.mysql.Set("gorm:table_options", "ENGINE=InnoDB").AutoMigrate(&MsgResultTable{}); err != nil {
		return err
	}
	return nil
}

func (c *Client) Ping() (bool, error) {
	db, _ := c.mysql.DB()
	err := db.Ping()
	if err != nil {
		return false, err
	}
	return false, err
}

func (c *Client) TakeMsgResult(key string) (message.Result, int64, error) {
	var result message.Result
	taskId := result.GetIdFromKey(key)
	r := c.mysql.Table("tb_message_result").Where("task_id = ?", taskId).Take(&result)
	return result, r.RowsAffected, r.Error
}

func (c *Client) SaveMsgResult(result message.Result) (error, int64) {

	var count int64
	tx := c.mysql.Begin()
	if err := tx.Table("tb_message_result").Where("task_id = ?", result.Id).Count(&count).Error; err != nil {
		tx.Rollback()
		return err, count
	}

	if count == 0 {
		r := tx.Table("tb_message_result").Create(&result)
		tx.Commit()
		return r.Error, r.RowsAffected
	}
	r := tx.Table("tb_message_result").Select("*").Omit("task_id").Updates(&result)
	tx.Commit()
	return r.Error, r.RowsAffected
}
