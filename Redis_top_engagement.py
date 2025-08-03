import redis
import time


redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_top_engagements(limit=10):
    top = redis_client.zrevrange("engagement_ranking",0,limit - 1,withscores=True)
    top_device = redis_client.zrevrange("most_devices_used",0,2,True)
    if not top :
        print("No data found")
        return
    elif not top_device:
        print("No data found")
        return
    print(f"top 3 device used")
    for rank , (device,score) in enumerate(top_device,1):
        print(f"{rank}. {device} counts {score}")
        
    
    print(f"\ntop {limit} content items in the last 10 mins")
    for rank,(content_type,score) in enumerate(top,1):
        engagement_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(score))
        print(f"{rank}. {content_type} (last engagement: {engagement_time})")

        
if __name__ == "__main__":
    while True:
        get_top_engagements()
        time.sleep(5)