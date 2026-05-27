# telegram-dm-collector-bot

一个用于 **收集 Telegram 用户资料与私信内容** 的私聊机器人项目。

## 当前能力

- 自动记录首次私聊用户资料
- 自动保存用户每一条私信
- 支持文本、图片、视频、文件、语音、贴纸等常见类型
- 可选：把用户私信同步转发给管理员
- 可选：自动回复“已收到”
- 管理员命令：
  - `/stats` 查看用户数 / 消息数
  - `/export` 导出 `users.csv` 和 `messages.csv`

## 项目结构

```text
telegram-dm-collector-bot/
├─ app/
│  ├─ bot.py
│  ├─ config.py
│  └─ database.py
├─ data/                # 运行后自动生成
├─ .env.example
├─ main.py
├─ README.md
└─ requirements.txt
```

## 使用方法

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`，至少填写：

```env
BOT_TOKEN=你的机器人Token
ADMIN_IDS=你的Telegram数字ID
```

### 3. 启动

```bash
python main.py
```

## 环境变量说明

- `BOT_TOKEN`：机器人 token
- `ADMIN_IDS`：管理员 ID，多个用英文逗号分隔
- `DB_PATH`：SQLite 数据库路径
- `FORWARD_TO_ADMINS`：是否把用户私信同步给管理员
- `SAVE_RAW_UPDATE`：是否保存原始消息 JSON
- `AUTO_REPLY_ENABLED`：是否自动回复
- `AUTO_REPLY_TEXT`：自动回复内容
- `WELCOME_TEXT`：用户 `/start` 时的欢迎语

## 数据说明

### users 表

保存用户基础信息：
- 用户 ID
- 用户名
- 姓名
- 语言
- 是否 Premium
- 首次出现时间
- 最后活跃时间
- `/start` 次数
- 消息数

### messages 表

保存每条私信：
- Telegram 消息 ID
- 用户 ID
- 消息类型
- 文本 / caption
- 附件 file_id
- media_group_id
- 原始 JSON（可选）
- 创建时间

## 适合后续继续扩展

后面你要继续往下写的话，我建议下一步加这几块：

1. **后台筛选面板**：按时间、用户名、关键词筛消息
2. **标签系统**：给用户打标签，比如“已跟进 / 高意向 / 垃圾私信”
3. **自动分流**：不同关键词自动推送给不同管理员
4. **Webhook + API**：把私信同步到你自己的管理后台
5. **广播/回访功能**：对已收集用户做二次触达

## 备注

这是一个先能跑的 MVP 骨架，重点是把“用户 + 私信内容”先稳定落库。
后续你要的话，我可以继续直接把：
- 管理后台
- 标签系统
- 关键词筛选
- 回复面板

一起给你补完整。
