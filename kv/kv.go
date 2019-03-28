package KV

import (
	"errors"
	"time"
)

var (
	mapping = make(map[string]*string)
	ttlMapping = make(map[string]*time.Time)
	getFunc func(string) (string, error)
	putFunc func(string, string, time.Duration) (error)
	deleteFunc func(string) (error)
	funcDisable bool
	defaultKey = "KV_key"
	defaultVal = "KV_val"
)


func Get(key string) (string, error) {
    if getFunc == nil || funcDisable {
    	val, ok := mapping[key]
    	if !ok {
    		return "", errors.New("record not found")
		} else {
			return *val, nil
		}
	} else {
		return getFunc(key)
	}
}

func Put(key, value string, ttl ...time.Duration) error {
    if putFunc == nil || funcDisable {
    	mapping[key] = &value
    	if len(ttl) > 0 {
    		deadline := time.Now().Add(ttl[0])
    	    ttlMapping[key] = &deadline
		}

    	return nil
	} else {
		var tl time.Duration
		if len(ttl) > 0 {
			tl = ttl[0]
		}
		return putFunc(key, value, tl)
	}

}

func Delete(key string) error {
    if deleteFunc == nil || funcDisable {
    	delete(mapping, key)
    	delete(ttlMapping, key)
    	return nil
	} else {
		return deleteFunc(key)
	}
}

func Init(get func(string) (string, error), put func(string, string, time.Duration) (error), delete func(string) (error)) (err error) {
	funcDisable = false
	getFunc = get
	putFunc = put
	deleteFunc = delete

	err = putFunc(defaultKey, defaultVal, 0)
	if err == nil {
		go healthCheck()
		go ttlCheck()
	}

	return
}

func InitLocalKV() {
	funcDisable = true
	go ttlCheck()
}

func healthCheck() {
	for {
		val, err := getFunc(defaultKey)
		if err != nil || val != defaultVal {
			if !funcDisable {
				funcDisable = true
			}

		} else {
			if funcDisable {
				funcDisable = false
			}
		}

		time.Sleep(time.Second)
	}
}


func ttlCheck() {
	for {
		for k, v := range ttlMapping {
			if (*v).Before(time.Now()) {
				delete(mapping, k)
				delete(ttlMapping, k)
			}
		}

		time.Sleep(time.Second)
	}
}

