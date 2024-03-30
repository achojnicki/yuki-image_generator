from adisconfig import adisconfig
from log import Log
from pika import BlockingConnection, PlainCredentials, ConnectionParameters
from json import loads, dumps
from pprint import pprint
from pathlib import Path
from uuid import uuid4
from os import mkdir
from pymongo import MongoClient
from yuki import prompt, db
from tqdm import tqdm

import openai
import requests

class Image_Generator:
    name="yuki-image_generator"

    def __init__(self):
        self._config=adisconfig('/opt/adistools/configs/yuki-image_generator.yaml')

        self._log=Log(
            parent=self,
            rabbitmq_host=self._config.rabbitmq.host,
            rabbitmq_port=self._config.rabbitmq.port,
            rabbitmq_user=self._config.rabbitmq.user,
            rabbitmq_passwd=self._config.rabbitmq.password,
            debug=self._config.log.debug,
            )

        self._prompt=prompt.Prompt()
        self._openai=openai.OpenAI(api_key=self._config.openai.api_key)

        self._rabbitmq_conn = BlockingConnection(
            ConnectionParameters(
                heartbeat=0,
                host=self._config.rabbitmq.host,
                port=self._config.rabbitmq.port,
                credentials=PlainCredentials(
                    self._config.rabbitmq.user,
                    self._config.rabbitmq.password
                )
            )
        )

        self._rabbitmq_channel = self._rabbitmq_conn.channel()
        self._rabbitmq_channel.basic_consume(
            queue='yuki-image_requests',
            auto_ack=True,
            on_message_callback=self._image_request
        )

        self._media_dir=Path(self._config.directories.media)
        self._renders_dir=Path(self._config.directories.renders)

        self._db=db.DB(self)


    def _download_image(self, url, file_name):
        with open(file_name, 'wb') as file:
            r=requests.get(url)
            file.write(r.content)



    def _notify_superivser(self, data):
        self._rabbitmq_channel.basic_publish(
                exchange="",
                routing_key="yuki-generation_finished",
                body=data
            )

    def _image_request(self, channel, method, properties, body):
        video_uuid=body.decode('utf8')
        video=self._db.get_video(video_uuid)

        for scene in tqdm(video['script']):
            while 1:
                try:
                    print(scene['image_description'])
                    response = self._openai.images.generate(
                        model=self._config.openai.model,
                        prompt=scene['image_description'],
                        size="1024x1024",
                        quality="standard",
                        n=1,
                    )
                    break
                
                except openai.BadRequestError:
                    print('Generation error. Trying again')


            file=self._media_dir.joinpath(video['video_uuid']).joinpath(f"scene_{video['script'].index(scene)+1}.png")
            self._download_image(
                response.data[0].url,
                file
                )

            video['script'][video['script'].index(scene)]['image']=str(file)

        self._db.update_video(video_uuid, video, 'image')
        self._notify_superivser(video['video_uuid'])

    def start(self):
        self._rabbitmq_channel.start_consuming()

    def stop(self):
        self._rabbitmq_channel.stop_consuming()

if __name__=="__main__":
    image_generator=Image_Generator()
    image_generator.start()
