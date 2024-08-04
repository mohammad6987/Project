from google_play_scraper import app,search,Sort
def is_app_available(app_name):
    try:
        # Search for the app
        results = search(
            app_name,
            lang='en',  # Language
            country='us',  # Country
            n_hits=5,  # Number of results to return
            
        )

        # Check if the app is in the search results
        for app in results:
            if app_name.lower() in app['title'].lower():
                print(f"App '{app_name}' is available on Google Play Store.")
                print(f"App ID: {app['appId']}")
                print(f"Developer: {app['developer']}")
                print(f"Score: {app['score']}")
                return True
        
        print(f"App '{app_name}' is not available on Google Play Store.")
        return False

    except Exception as e:
        print(f"Error occurred: {e}")
        return False

# Example usage
app_name = input("Enter the app name: ")
res = is_app_available(app_name)

