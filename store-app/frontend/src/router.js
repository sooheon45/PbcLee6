
import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router);


import StorageManager from "./components/listers/StorageCards"
import StorageDetail from "./components/listers/StorageDetail"



export default new Router({
    // mode: 'history',
    base: process.env.BASE_URL,
    routes: [
            {
                path: '/storages',
                name: 'StorageManager',
                component: StorageManager
            },
            {
                path: '/storages/:id',
                name: 'StorageDetail',
                component: StorageDetail
            },




    ]
})
