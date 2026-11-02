import asyncio
import json
from datetime import datetime
import aiohttp

POSTS_API_URL = "https://jsonplaceholder.typicode.com/posts"
USERS_API_URL = "https://jsonplaceholder.typicode.com/users"


async def fetch_data(url: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore) -> list | dict | None:
    async with semaphore: 
        try:
            timeout = aiohttp.ClientTimeout(total=5)  # Таймаут 5 секунд
            async with session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"[Ошибка HTTP {response.status}] Не удалось получить данные от {url}")

        except asyncio.TimeoutError:
            print(f"[Таймаут] Сервер не ответил за 5 секунд: {url}")
        except aiohttp.ClientConnectorError as err:
            print(f"[Ошибка подключения] Нет связи с сервером {url}: {err}")
        except aiohttp.ClientError as err:
            print(f"[Сетевая ошибка] Ошибка aiohttp: {err}")
        except Exception as err:
            print(f"[Непредвиденная ошибка] {err} при запросе {url}")

    return None


async def get_data_simultaneously(semaphore: asyncio.Semaphore) -> dict:
    async with aiohttp.ClientSession() as session:
        task_posts = fetch_data(POSTS_API_URL, session, semaphore)
        task_users = fetch_data(USERS_API_URL, session, semaphore)

        posts, users = await asyncio.gather(task_posts, task_users)

        return {
            "timestamp": datetime.now().isoformat(),
            "posts": posts,
            "users": users
        }


def save_data(data: dict, filename: str = "data.json"):
    try:
        history = []
        try:
            with open(filename, "r", encoding="utf-8") as f:
                history = json.load(f)
                if not isinstance(history, list):
                    history = [history]
        except (FileNotFoundError, json.JSONDecodeError):
            history = []

        history.append(data)

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=4)

        print(f"[Успех] Данные сохранены в файл: {filename}")
    except Exception as e:
        print(f"[Ошибка сохранения] Не удалось сохранить файл: {e}")


def display_results(data: dict):
    print("\n" + "=" * 50)
    print(f"Время сбора: {data['timestamp']}")

    posts = data.get("posts")
    if posts and isinstance(posts, list):
        print(f"Посты получены: {len(posts)} шт.")
        print(f"Пример первого поста: \"{posts[0].get('title')}\"")
    else:
        print("Посты: не удалось получить")

    users = data.get("users")
    if users and isinstance(users, list):
        print(f"Пользователи получены: {len(users)} чел.")
        print(f"Пример первого пользователя: {users[0].get('name')} ({users[0].get('email')})")
    else:
        print("Пользователи: не удалось получить")

    print("=" * 50 + "\n")


async def main(periodic: bool = False, interval_seconds: int = 600):
    semaphore = asyncio.Semaphore(3)

    if not periodic:
        print("[Старт] Запуск разового сбора данных с JSONPlaceholder...")
        data = await get_data_simultaneously(semaphore)
        display_results(data)
        save_data(data)
        print("[Готово] Программа завершила работу.")
    else:
        print(f"[Старт] Циклический сбор данных каждые {interval_seconds} сек. (Ctrl+C для выхода)")
        while True:
            data = await get_data_simultaneously(semaphore)
            display_results(data)
            save_data(data)
            print(f"Ожидание {interval_seconds} секунд перед следующим запуском...")
            await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(main(periodic=False))