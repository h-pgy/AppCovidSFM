def headers_no_chache(request_obj):

    request_obj.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    request_obj.headers["Pragma"] = "no-cache"
    request_obj.headers["Expires"] = "0"
    request_obj.headers['Cache-Control'] = 'public, max-age=0'

