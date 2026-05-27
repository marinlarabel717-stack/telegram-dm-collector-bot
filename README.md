# telegram-dm-collector-bot

一个用于 **收集 Telegram 用户资料与私信内容** 的私聊机器人项目。

## 当前技术约定

- 基于 **python-telegram-bot 20+**
- 正文消息统一走：`<tg-emoji ...>` + `parse_mode='HTML'`
- 按钮图标统一走：`api_kwargs['icon_custom_emoji_id']`
- 机器人默认按 **高级会员 Bot** 的展示风格来组织文案和按钮

## 当前能力

- 自动记录首次私聊用户资料
- 自动保存用户每一条私信
- 支持文本、图片、视频、文件、语音、贴纸等常见类型
- 可选：把用户私信同步转发给管理员
- 可选：自动回复“已收到”
- 管理员命令：
  - `/stats` 查看用户数 / 消息数
  - `/export` 导出 `users.csv` 和 `messages.csv`
- 管理员欢迎页已接入会员 emoji 按钮：
  - 查看统计
  - 导出数据

## 项目结构

```text
telegram-dm-collector-bot/
├─ app/
│  ├─ bot.py
│  ├─ config.py
│  ├─ database.py
│  └─ emoji.py
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
- `EMOJI_WELCOME_ID`：欢迎标题 custom emoji id
- `EMOJI_INBOX_ID`：收件箱/正文提示 custom emoji id
- `EMOJI_STATS_ID`：统计按钮 custom emoji id
- `EMOJI_EXPORT_ID`：导出按钮 custom emoji id
- `EMOJI_SUCCESS_ID`：成功提示 custom emoji id

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

## 下一步

你后面继续给功能需求，我就在这个仓库里继续往下加。
目前这版先把 **PTB 20+ + 会员 emoji 正文链路 + 会员按钮图标链路** 先固定下来。
